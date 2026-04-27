"""
抓取 rlsbb 站点发布信息

配置文件 ``config/scrapy_rls.json`` 需要提供：
- ``rls_url``: 站点根地址。
- ``output_dir``: 生成 ``.rls`` 文件的输出目录。
- ``foreign_end_titles``: ``foreign-movies`` 流程的截止标题列表。
- ``movie_end_titles``: ``movies`` 流程的截止标题列表。
- ``rls_verification_url``: 可选，手动通过 Cloudflare 时使用的验证入口页。

主流程：
1. 先顺序翻页，把两条列表流程的新帖子统一写入 Redis 队列。
2. 再从 Redis 队列中并发抓取详情页并写出 ``.rls`` 文件。
3. 两条列表都扫完且详情任务清空后，再回写两套 ``end_titles``。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import re
import sys
import time

import redis
import requests
from bs4 import BeautifulSoup
from retrying import retry

from my_module import normalize_release_title_for_filename, read_json_to_dict, sanitize_filename, update_json_config, write_list_to_file
from scrapy_redis import deserialize_payload, drain_queue, get_redis_client, push_items_to_queue, serialize_payload
from sort_movie_ops import extract_imdb_id_from_links

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG_PATH = 'config/scrapy_rls.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

RLS_URL = CONFIG['rls_url']  # rlsbb 地址
RLS_VERIFICATION_URL = CONFIG.get('rls_verification_url')  # 手动过 CF 的验证入口页
RLS_COOKIE = CONFIG['rls_cookie']  # 用户甜甜
REQUEST_HEAD = CONFIG['request_head']  # 请求头
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录
THREAD_NUMBER = CONFIG.get('thread_number', CONFIG.get('max_workers', 40))  # 线程数
END_TITLES_KEEP_COUNT = 2
MAX_EMPTY_PAGE_RETRIES = 3

REDIS_PENDING_KEY = CONFIG.get('redis_pending_key', 'rls_pending')  # 待处理队列
REDIS_PROCESSING_KEY = CONFIG.get('redis_processing_key', 'rls_processing')  # 处理中队列
REDIS_SEEN_KEY = CONFIG.get('redis_seen_key', 'rls_seen')  # 已入队项目集合
REDIS_FOREIGN_SCAN_PAGE_KEY = CONFIG.get('redis_foreign_scan_page_key', 'rls_foreign_scan_page')  # foreign 扫描断点
REDIS_MOVIE_SCAN_PAGE_KEY = CONFIG.get('redis_movie_scan_page_key', 'rls_movie_scan_page')  # movie 扫描断点
REDIS_FOREIGN_SCAN_COMPLETE_KEY = CONFIG.get('redis_foreign_scan_complete_key', 'rls_foreign_scan_complete')  # foreign 扫描完成
REDIS_MOVIE_SCAN_COMPLETE_KEY = CONFIG.get('redis_movie_scan_complete_key', 'rls_movie_scan_complete')  # movie 扫描完成
REDIS_FOREIGN_NEXT_END_TITLES_KEY = CONFIG.get('redis_foreign_next_end_titles_key', 'rls_foreign_next_end_titles')  # foreign 下一轮截止标题
REDIS_MOVIE_NEXT_END_TITLES_KEY = CONFIG.get('redis_movie_next_end_titles_key', 'rls_movie_next_end_titles')  # movie 下一轮截止标题

REQUEST_HEAD["Cookie"] = RLS_COOKIE  # 请求头加入认证


class RlsCloudflareError(RuntimeError):
    """RLS 请求命中 Cloudflare 验证页，通常意味着 Cookie 已失效。"""


def normalize_rls_title(title: str) -> str:
    """统一截止标题和列表页标题的比较格式。"""
    return title.strip().replace(" ⭐", "").replace(" ", ".")


def is_rls_cloudflare_challenge(response: requests.Response) -> bool:
    """判断响应是否为 Cloudflare 验证页。"""
    text = getattr(response, "text", "")
    if not isinstance(text, str):
        text = str(text)
    text = text.lower()
    markers = (
        "<title>just a moment",
        "challenges.cloudflare.com",
        "cf_chl",
        "challenge-platform",
        "cf-browser-verification",
        "<title>attention required!",
    )
    return any(marker in text for marker in markers)


def get_current_end_titles(f_mode: bool = True) -> list[str]:
    """按当前模式读取并清洗截止标题列表。"""
    key = "foreign_end_titles" if f_mode else "movie_end_titles"
    return [
        normalize_rls_title(title)
        for title in read_json_to_dict(CONFIG_PATH).get(key, [])
        if isinstance(title, str) and title.strip()
    ]


def enqueue_rls_single_mode(start_page: int, f_mode: bool, redis_client: redis.Redis) -> None:
    """顺序翻页，把单条列表流程的新帖子写入 Redis 待处理队列。"""
    end_titles = get_current_end_titles(f_mode)
    if not end_titles:
        raise ValueError("至少需要提供一个截止标题")

    if f_mode:
        category = "foreign-movies"
        scan_page_key = REDIS_FOREIGN_SCAN_PAGE_KEY
        scan_complete_key = REDIS_FOREIGN_SCAN_COMPLETE_KEY
        next_titles_key = REDIS_FOREIGN_NEXT_END_TITLES_KEY
        log_prefix = "RLS foreign"
    else:
        category = "movies"
        scan_page_key = REDIS_MOVIE_SCAN_PAGE_KEY
        scan_complete_key = REDIS_MOVIE_SCAN_COMPLETE_KEY
        next_titles_key = REDIS_MOVIE_NEXT_END_TITLES_KEY
        log_prefix = "RLS movie"

    if redis_client.get(scan_complete_key) == "1":
        logger.info(f"{log_prefix} 列表扫描已完成，跳过入队阶段")
        return

    saved_page = redis_client.get(scan_page_key)
    current_page = int(saved_page) if saved_page is not None else start_page
    if saved_page is None:
        redis_client.set(scan_page_key, str(current_page))
    else:
        logger.info(f"{log_prefix} 从第 {current_page} 页继续扫描")

    while True:
        logger.info(f"{log_prefix} 抓取第 {current_page} 页")
        url = f"{RLS_URL}category/{category}/page/{current_page}/?s="
        empty_retry_count = 0
        while True:
            response = get_rls_response(url)
            result_list = parse_rls_response(response)
            if result_list:
                break
            empty_retry_count += 1
            if empty_retry_count >= MAX_EMPTY_PAGE_RETRIES:
                raise RuntimeError(
                    f"{log_prefix} 第 {current_page} 页连续 {empty_retry_count} 次解析为空，"
                    f"网站结构可能已变更或仍在 Cloudflare 验证页"
                )
            time.sleep(3)
            logger.warning(f"{log_prefix} 第 {current_page} 页解析为空，等待 3 秒后重试")
        logger.info(f"{log_prefix} 共 {len(result_list)} 个结果")

        if redis_client.get(next_titles_key) is None:
            redis_client.set(
                next_titles_key,
                serialize_payload(
                    {"titles": [item["title"] for item in result_list[:END_TITLES_KEEP_COUNT] if item["title"]]}
                ),
            )

        enqueued_count = push_items_to_queue(
            redis_client,
            result_list,
            seen_key=REDIS_SEEN_KEY,
            pending_key=REDIS_PENDING_KEY,
            unique_value=lambda item: item["url"],
            serializer=lambda item: serialize_payload(
                {
                    "title": item["title"],
                    "url": item["url"],
                }
            ),
        )
        logger.info(f"{log_prefix} 第 {current_page} 页解析 {len(result_list)} 条，入队 {enqueued_count} 条")

        if any(item["title"] in end_titles for item in result_list):
            redis_client.set(scan_complete_key, "1")
            redis_client.set(scan_page_key, str(current_page + 1))
            logger.info(f"{log_prefix} 列表扫描完成")
            break

        current_page += 1
        redis_client.set(scan_page_key, str(current_page))
        logger.info("-" * 255)


def enqueue_rls_posts(start_page: int = 1, redis_client: redis.Redis | None = None) -> None:
    """顺序扫描两条列表流程，把新帖子统一写入 Redis 待处理队列。"""
    if redis_client is None:
        redis_client = get_redis_client()

    enqueue_rls_single_mode(start_page, True, redis_client)
    logger.warning("-" * 255)
    enqueue_rls_single_mode(start_page, False, redis_client)


def recover_rls_processing_when_pending_is_empty(redis_client: redis.Redis) -> int:
    """启动时若待处理为空但处理中有残留，则回退到待处理并继续运行。"""
    if redis_client.llen(REDIS_PENDING_KEY) or not redis_client.llen(REDIS_PROCESSING_KEY):
        return 0

    recovered_count = 0
    while True:
        payload = redis_client.rpoplpush(REDIS_PROCESSING_KEY, REDIS_PENDING_KEY)
        if not payload:
            break
        recovered_count += 1
    logger.warning(f"RLS 检测到待处理为空但处理中残留 {recovered_count} 条，已回退到待处理队列并继续运行")
    return recovered_count


def drain_rls_queue(redis_client: redis.Redis | None = None) -> None:
    """从 Redis 队列中取帖子，使用多线程访问详情页并写出 ``.rls`` 文件。"""
    if redis_client is None:
        redis_client = get_redis_client()

    drain_queue(
        redis_client,
        pending_key=REDIS_PENDING_KEY,
        processing_key=REDIS_PROCESSING_KEY,
        max_workers=THREAD_NUMBER,
        worker=visit_rls_url,
        deserialize=deserialize_payload,
        logger=logger,
        queue_label="RLS",
        identify_item=lambda info: info["url"],
        abort_on_exception=lambda exc: isinstance(exc, RlsCloudflareError),
        recover_processing_on_start=False,
        keep_failed_in_processing=True,
    )


def finalize_rls_run(redis_client: redis.Redis | None = None) -> None:
    """在列表扫描和详情任务都结束后，回写两套 ``end_titles`` 并清理运行状态。"""
    if redis_client is None:
        redis_client = get_redis_client()

    if redis_client.get(REDIS_FOREIGN_SCAN_COMPLETE_KEY) != "1" or redis_client.get(REDIS_MOVIE_SCAN_COMPLETE_KEY) != "1":
        logger.info("RLS 列表扫描尚未完成，暂不回写 end_titles")
        return

    pending_count = redis_client.llen(REDIS_PENDING_KEY)
    if pending_count:
        logger.info("RLS 队列仍有未完成任务，暂不回写 end_titles")
        return

    processing_count = redis_client.llen(REDIS_PROCESSING_KEY)
    if processing_count:
        logger.warning(f"RLS 待处理已空，但处理中仍有 {processing_count} 条，已保留处理中队列，请直接重跑")
        return

    foreign_titles_payload = redis_client.get(REDIS_FOREIGN_NEXT_END_TITLES_KEY)
    movie_titles_payload = redis_client.get(REDIS_MOVIE_NEXT_END_TITLES_KEY)
    if not foreign_titles_payload or not movie_titles_payload:
        logger.warning("RLS 未记录新的 end_titles，跳过配置更新")
        return

    update_json_config(CONFIG_PATH, "foreign_end_titles", deserialize_payload(foreign_titles_payload)["titles"])
    update_json_config(CONFIG_PATH, "movie_end_titles", deserialize_payload(movie_titles_payload)["titles"])
    redis_client.delete(
        REDIS_PENDING_KEY,
        REDIS_PROCESSING_KEY,
        REDIS_FOREIGN_SCAN_PAGE_KEY,
        REDIS_MOVIE_SCAN_PAGE_KEY,
        REDIS_FOREIGN_SCAN_COMPLETE_KEY,
        REDIS_MOVIE_SCAN_COMPLETE_KEY,
        REDIS_FOREIGN_NEXT_END_TITLES_KEY,
        REDIS_MOVIE_NEXT_END_TITLES_KEY,
    )


def scrapy_rls(start_page: int = 1) -> None:
    """
    先翻页入 Redis，再从 Redis 队列中多线程抓取详情。
    """
    logger.info("抓取 rlsbb 站点发布信息")
    redis_client = get_redis_client()
    try:
        recover_rls_processing_when_pending_is_empty(redis_client)
        enqueue_rls_posts(start_page=start_page, redis_client=redis_client)
        drain_rls_queue(redis_client=redis_client)
    finally:
        finalize_rls_run(redis_client=redis_client)


def should_retry_rls_request(exc: Exception) -> bool:
    """Cloudflare 验证页属于致命状态，不做无意义重试。"""
    return not isinstance(exc, RlsCloudflareError)


@retry(
    stop_max_attempt_number=150,
    wait_random_min=1000,
    wait_random_max=10000,
    retry_on_exception=should_retry_rls_request,
)
def get_rls_response(url: str) -> requests.Response:
    """请求流程"""
    response = requests.get(url, timeout=35, headers=REQUEST_HEAD)
    response.encoding = 'utf-8'
    if is_rls_cloudflare_challenge(response):
        verification_url = RLS_VERIFICATION_URL or url
        raise RlsCloudflareError(
            f"rls Cookie 已失效或触发 Cloudflare 验证，请先在浏览器手动通过后重跑：{verification_url}"
        )
    if response.status_code == 403:
        sys.exit(f"被墙了 {response.status_code}：{url}")
    elif response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    return response


def parse_rls_response(response: requests.Response) -> list:
    """解析响应文本"""
    soup = BeautifulSoup(response.text, "html.parser")
    results = []
    for title_div in soup.find_all("div", class_="p-c p-c-title"):
        a_tag = title_div.find("h2").find("a")
        results.append(
            {
                "title": normalize_rls_title(a_tag.get_text(strip=True)),
                "url": a_tag.get("href"),
            }
        )

    return results


def extract_rls_imdb_id(soup: BeautifulSoup) -> str:
    """先匹配标准 IMDb 链接，找不到再宽松回退到任意 tt 编号。"""
    href_list = [a['href'] for a in soup.find_all('a', href=True)]
    return extract_imdb_id_from_links(href_list) or ""


def visit_rls_url(result_item: dict):
    """访问详情页"""
    url = result_item["url"]
    logger.info(f"访问 {url}")
    response = get_rls_response(url)
    soup = BeautifulSoup(response.text, 'lxml')
    imdb_id = extract_rls_imdb_id(soup)

    result_item["imdb"] = imdb_id
    file_name = normalize_release_title_for_filename(result_item['title'])
    file_name = sanitize_filename(file_name)
    file_name = f"{file_name} - rls [{imdb_id}].rls"
    path = os.path.join(OUTPUT_DIR, file_name)
    write_list_to_file(path, [url])
