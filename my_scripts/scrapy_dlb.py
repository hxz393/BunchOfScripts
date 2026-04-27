"""
抓取 dlb 站点发布信息

配置文件 ``config/scrapy_dlb.json`` 需要提供：
- ``dlb_url``: 站点根地址。
- ``output_dir``: 生成 ``.dlb`` 文件的输出目录。
- ``dlb_cookie``: 访问站点所需的 Cookie。
- ``request_head``: 请求头模板。
- ``end_titles``: 分页抓取时用于判断“已经追到旧数据”的截止标题列表。
  正常跑完一轮后，脚本会把首次访问页的前两个标题写回这里，供下次运行使用。
- ``thread_number`` / ``max_workers``: 详情页并发抓取线程数，未提供时默认 ``20``。

主流程：
1. 先顺序翻页，把新列表项写入 Redis 待处理队列。
2. 再从 Redis 队列中并发抓取详情页并写出 ``.dlb`` 文件。
3. 当列表页中出现任一截止标题时停止翻页。
4. 只有列表扫描完成且详情任务全部成功清空后，才回写新的 ``end_titles``。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2027, hxz393. 保留所有权利。
"""
import logging
import os
import re
from typing import Dict, Iterable, List

import redis
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from retrying import retry
from urllib3.util.retry import Retry

from my_module import normalize_release_title_for_filename, read_json_to_dict, sanitize_filename, update_json_config, write_list_to_file
from scrapy_redis import deserialize_payload, drain_queue, get_redis_client, push_items_to_queue, serialize_payload

CONFIG_PATH = 'config/scrapy_dlb.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

DLB_URL = CONFIG['dlb_url']  # dlb 地址
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录
DLB_COOKIE = CONFIG['dlb_cookie']  # 用户甜甜
REQUEST_HEAD = CONFIG['request_head']  # 请求头
REQUEST_HEAD["Cookie"] = DLB_COOKIE  # 请求头加入认证
THREAD_NUMBER = CONFIG.get('thread_number', CONFIG.get('max_workers', 20))

LEGACY_END_TITLE = "Tawai.A.voice.from.the.forest.2017.1080p.BluRay.REMUX.AVC.DTS-HD.MA.5.1-EPSiLON – 22.4 GB"
END_TITLES_KEEP_COUNT = 2

REDIS_PENDING_KEY = CONFIG.get('redis_pending_key', 'dlb_pending')  # 待处理队列
REDIS_PROCESSING_KEY = CONFIG.get('redis_processing_key', 'dlb_processing')  # 处理中队列
REDIS_SEEN_KEY = CONFIG.get('redis_seen_key', 'dlb_seen')  # 已入队项目集合
REDIS_SCAN_PAGE_KEY = CONFIG.get('redis_scan_page_key', 'dlb_scan_page')  # 列表扫描断点页码
REDIS_SCAN_COMPLETE_KEY = CONFIG.get('redis_scan_complete_key', 'dlb_scan_complete')  # 列表扫描完成标记
REDIS_NEXT_END_TITLES_KEY = CONFIG.get('redis_next_end_titles_key', 'dlb_next_end_titles')  # 下一轮截止标题

REQUEST_TIMEOUT_SECONDS = 30
BLOCKED_PAGE_MIN_LENGTH = 10000
SESSION_PROXIES = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890",
}

IMDB_URL_PATTERN = re.compile(r"https?://(?:www\.)?imdb\.com/title/(tt\d+)")
IMDB_ID_PATTERN = re.compile(r"(tt\d+)")

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()


def build_retry_strategy() -> Retry:
    """构造 requests 适配器使用的重试策略。"""
    return Retry(
        total=15,
        status_forcelist=[502],
        allowed_methods=["POST", "GET"],
        backoff_factor=1,
    )


def build_dlb_session() -> requests.Session:
    """创建带连接池、重试和代理配置的会话。"""
    adapter = HTTPAdapter(max_retries=build_retry_strategy(), pool_connections=20, pool_maxsize=40)
    request_session = requests.Session()
    request_session.proxies = SESSION_PROXIES.copy()
    request_session.mount("http://", adapter)
    request_session.mount("https://", adapter)
    return request_session


session = build_dlb_session()


def normalize_end_titles(titles: Iterable[str]) -> List[str]:
    """清洗配置中的截止标题列表。"""
    return [title.strip() for title in titles if isinstance(title, str) and title.strip()]


def get_current_end_titles() -> List[str]:
    """读取当前生效的截止标题列表，缺失时回退到历史单标题默认值。"""
    config = read_json_to_dict(CONFIG_PATH)
    titles = config.get("end_titles") or CONFIG.get("end_titles") or [LEGACY_END_TITLE]
    return normalize_end_titles(titles)


def build_dlb_page_url(page_number: int) -> str:
    """根据页码构造 DLB 电影列表页 URL。"""
    return f"{DLB_URL}/cat/movie/page/{page_number}/"


def should_stop_scrapy(result_list: List[Dict[str, str]], end_titles: List[str]) -> bool:
    """当前批次命中任一截止标题时返回 True。"""
    if not end_titles:
        return False

    return any(result_item.get("title") in end_titles for result_item in result_list)


def select_next_end_titles(result_list: List[Dict[str, str]], keep_count: int = END_TITLES_KEEP_COUNT) -> List[str]:
    """从首次访问页中选出下一轮要写回配置的前几个标题。"""
    titles = [result_item.get("title", "").strip() for result_item in result_list if result_item.get("title", "").strip()]
    return titles[:keep_count]


def serialize_dlb_post(result_item: Dict[str, str]) -> str:
    """将列表项序列化为 Redis 队列任务。"""
    return serialize_payload(
        {
            "title": result_item["title"],
            "link": result_item["link"],
            "size": result_item["size"],
        }
    )


def enqueue_dlb_posts(start_page: int = 1, redis_client: redis.Redis | None = None) -> None:
    """顺序翻页，收集新帖子并写入 Redis 待处理队列。"""
    if redis_client is None:
        redis_client = get_redis_client()

    end_titles = get_current_end_titles()
    if not end_titles:
        raise ValueError("至少需要提供一个截止标题")

    if redis_client.get(REDIS_SCAN_COMPLETE_KEY) == "1":
        logger.info("DLB 列表扫描已完成，跳过入队阶段")
        return

    saved_page = redis_client.get(REDIS_SCAN_PAGE_KEY)
    current_page = int(saved_page) if saved_page is not None else start_page
    if saved_page is None:
        redis_client.set(REDIS_SCAN_PAGE_KEY, str(current_page))
    else:
        logger.info(f"DLB 从第 {current_page} 页继续扫描")

    while True:
        logger.info(f"抓取第 {current_page} 页")
        response = get_dlb_response(build_dlb_page_url(current_page))
        result_list = parse_dlb_response(response)
        logger.info(f"共 {len(result_list)} 个结果")

        if redis_client.get(REDIS_NEXT_END_TITLES_KEY) is None:
            redis_client.set(
                REDIS_NEXT_END_TITLES_KEY,
                serialize_payload({"titles": select_next_end_titles(result_list)}),
            )

        enqueued_count = push_items_to_queue(
            redis_client,
            result_list,
            seen_key=REDIS_SEEN_KEY,
            pending_key=REDIS_PENDING_KEY,
            unique_value=lambda item: item["link"],
            serializer=serialize_dlb_post,
        )
        logger.info(f"第 {current_page} 页解析 {len(result_list)} 条，入队 {enqueued_count} 条")

        if should_stop_scrapy(result_list, end_titles):
            redis_client.set(REDIS_SCAN_COMPLETE_KEY, "1")
            redis_client.set(REDIS_SCAN_PAGE_KEY, str(current_page + 1))
            logger.info("DLB 列表扫描完成")
            break

        current_page += 1
        redis_client.set(REDIS_SCAN_PAGE_KEY, str(current_page))
        logger.warning("-" * 255)


def recover_dlb_processing_when_pending_is_empty(redis_client: redis.Redis) -> int:
    """启动时若待处理为空但处理中有残留，则回退到待处理并继续运行。"""
    if redis_client.llen(REDIS_PENDING_KEY) or not redis_client.llen(REDIS_PROCESSING_KEY):
        return 0

    recovered_count = 0
    while True:
        payload = redis_client.rpoplpush(REDIS_PROCESSING_KEY, REDIS_PENDING_KEY)
        if not payload:
            break
        recovered_count += 1

    logger.warning(f"DLB 检测到待处理为空但处理中残留 {recovered_count} 条，已回退到待处理队列并继续运行")
    return recovered_count


def drain_dlb_queue(redis_client: redis.Redis | None = None) -> None:
    """从 Redis 队列中取帖子，使用多线程访问详情页并写出 ``.dlb`` 文件。"""
    if redis_client is None:
        redis_client = get_redis_client()

    drain_queue(
        redis_client,
        pending_key=REDIS_PENDING_KEY,
        processing_key=REDIS_PROCESSING_KEY,
        max_workers=THREAD_NUMBER,
        worker=visit_dlb_url,
        deserialize=deserialize_payload,
        logger=logger,
        queue_label="DLB",
        identify_item=lambda info: info["link"],
        recover_processing_on_start=False,
        keep_failed_in_processing=True,
    )


def finalize_dlb_run(redis_client: redis.Redis | None = None) -> None:
    """在列表扫描和详情任务都结束后，回写 ``end_titles`` 并清理运行状态。"""
    if redis_client is None:
        redis_client = get_redis_client()

    if redis_client.get(REDIS_SCAN_COMPLETE_KEY) != "1":
        logger.info("DLB 列表扫描尚未完成，暂不回写 end_titles")
        return

    pending_count = redis_client.llen(REDIS_PENDING_KEY)
    if pending_count:
        logger.info("DLB 队列仍有未完成任务，暂不回写 end_titles")
        return

    processing_count = redis_client.llen(REDIS_PROCESSING_KEY)
    if processing_count:
        logger.warning(f"DLB 待处理已空，但处理中仍有 {processing_count} 条，已保留处理中队列，请直接重跑")
        return

    next_titles_payload = redis_client.get(REDIS_NEXT_END_TITLES_KEY)
    if not next_titles_payload:
        logger.warning("DLB 未记录新的 end_titles，跳过配置更新")
        return

    update_json_config(CONFIG_PATH, "end_titles", deserialize_payload(next_titles_payload)["titles"])
    redis_client.delete(
        REDIS_PENDING_KEY,
        REDIS_PROCESSING_KEY,
        REDIS_SCAN_PAGE_KEY,
        REDIS_SCAN_COMPLETE_KEY,
        REDIS_NEXT_END_TITLES_KEY,
    )


def scrapy_dlb(start_page: int = 1) -> None:
    """先翻页入 Redis，再从 Redis 队列中多线程抓取详情。"""
    logger.info("抓取 dlb 站点发布信息")
    redis_client = get_redis_client()
    try:
        recover_dlb_processing_when_pending_is_empty(redis_client)
        enqueue_dlb_posts(start_page=start_page, redis_client=redis_client)
        drain_dlb_queue(redis_client=redis_client)
    finally:
        finalize_dlb_run(redis_client=redis_client)


@retry(stop_max_attempt_number=15, wait_random_min=15000, wait_random_max=20000)
def get_dlb_response(url: str, request_session: requests.Session | None = None) -> requests.Response:
    """请求页面并校验状态码与正文长度。"""
    logger.info(f"访问 {url}")
    request_client = request_session or session
    response = request_client.get(url, timeout=REQUEST_TIMEOUT_SECONDS, verify=False, headers=REQUEST_HEAD)
    if response.status_code != 200:
        logger.error(f"请求失败，重试 {response.status_code}：{url}")
        raise Exception("请求失败")

    if len(response.text) < BLOCKED_PAGE_MIN_LENGTH:
        logger.error(f"请求被封锁，重试：{url}\n{response.text}")
        raise Exception("请求被封锁")

    return response


def parse_dlb_response(response: requests.Response) -> List[Dict[str, str]]:
    """解析 DLB 单页列表，输出 ``title/link/size`` 字典列表。"""
    soup = BeautifulSoup(response.text, "html.parser")
    results = []

    for block in soup.select("div.movies_block"):
        title_tag = block.select_one("span.movie_title_list_text")
        title = title_tag.get_text(strip=True) if title_tag else ""

        link_tag = block.select_one("div.movie_title_list a")
        href = link_tag["href"] if link_tag and "href" in link_tag.attrs else ""

        size_tag = block.select_one("div.type_banner_size")
        size = size_tag.get_text(strip=True) if size_tag else ""

        results.append(
            {
                "title": title,
                "link": DLB_URL + href,
                "size": size.replace(' ', ''),
            }
        )

    return results


def extract_dlb_imdb_id(soup: BeautifulSoup) -> str:
    """从详情页所有链接中提取 IMDb 编号。"""
    return extract_dlb_imdb_id_from_links(a["href"] for a in soup.find_all("a", href=True))


def extract_dlb_imdb_id_from_links(hrefs: Iterable[str]) -> str:
    """优先匹配标准 IMDb 链接，失败时回退到宽松 ``tt`` 编号匹配。"""
    fallback_imdb_id = ""
    for href in hrefs:
        match = IMDB_URL_PATTERN.search(href)
        if match:
            return match.group(1)

        if not fallback_imdb_id:
            match = IMDB_ID_PATTERN.search(href)
            if match:
                fallback_imdb_id = match.group(1)

    return fallback_imdb_id


def build_dlb_output_filename(result_item: Dict[str, str], imdb_id: str) -> str:
    """根据发布信息拼装输出文件名，格式为 ``标题 (体积)[IMDb].dlb``。"""
    file_name = normalize_release_title_for_filename(result_item["title"])
    file_name = sanitize_filename(file_name)
    return f"{file_name} ({result_item['size']})[{imdb_id}].dlb"


def visit_dlb_url(result_item: Dict[str, str]) -> None:
    """访问详情页并写出对应的 ``.dlb`` 文件。"""
    url = result_item["link"]
    detail_session = build_dlb_session()
    response = get_dlb_response(url, request_session=detail_session)
    soup = BeautifulSoup(response.text, 'lxml')
    imdb_id = extract_dlb_imdb_id(soup)
    path = os.path.join(OUTPUT_DIR, build_dlb_output_filename(result_item, imdb_id))
    write_list_to_file(path, [url])
