"""
抓取 hde 站点发布信息

配置文件 ``config/scrapy_hde.json`` 需要提供：
- ``hde_url``: 站点根地址。
- ``output_dir``: 生成 ``.rls`` 文件的输出目录。
- ``end_titles``: 分页抓取时用于判断“已经追到旧数据”的截止标题列表。
  正常跑完一轮后，脚本会把首次访问页的前两个标题写回这里，供下次运行使用。
- ``max_workers`` / ``thread_number``: 详情页并发抓取线程数。
- ``default_release_size``: 标题里提取不到体积信息时使用的默认值。
- ``request_timeout_seconds``: 单次 HTTP 请求超时秒数。
- ``retry_max_attempts`` / ``retry_wait_min_ms`` / ``retry_wait_max_ms``:
  ``get_hde_response`` 的重试参数。

主流程：
1. 先顺序翻页，把新列表项写入 Redis 待处理队列。
2. 再从 Redis 队列中并发抓取详情页并写出 ``.rls`` 文件。
3. 当列表页中出现任一截止标题时停止翻页。
4. 只有列表扫描完成且详情任务全部成功清空后，才回写新的 ``end_titles``。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import logging
import os
import re
from typing import Dict, Iterable, List
from urllib.parse import urljoin, urlparse

import redis
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from retrying import retry
from urllib3.util.retry import Retry

from my_module import normalize_release_title_for_filename, read_json_to_dict, sanitize_filename, update_json_config, write_list_to_file
from scrapy_redis import deserialize_payload, drain_queue, get_redis_client, push_items_to_queue, serialize_payload
from sort_movie_ops import extract_imdb_id_from_links as extract_shared_imdb_id_from_links

logger = logging.getLogger(__name__)


CONFIG_PATH = 'config/scrapy_hde.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

HDE_URL = CONFIG['hde_url']  # hde 地址
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录
END_TITLES = [title.strip() for title in CONFIG['end_titles'] if isinstance(title, str) and title.strip()]
DEFAULT_MAX_WORKERS = CONFIG.get('thread_number', CONFIG['max_workers'])
DEFAULT_RELEASE_SIZE = CONFIG['default_release_size']
REQUEST_TIMEOUT_SECONDS = CONFIG['request_timeout_seconds']
RETRY_MAX_ATTEMPTS = CONFIG['retry_max_attempts']
RETRY_WAIT_MIN_MS = CONFIG['retry_wait_min_ms']
RETRY_WAIT_MAX_MS = CONFIG['retry_wait_max_ms']
END_TITLES_KEEP_COUNT = 2

REDIS_PENDING_KEY = CONFIG.get('redis_pending_key', 'hde_pending')  # 待处理队列
REDIS_PROCESSING_KEY = CONFIG.get('redis_processing_key', 'hde_processing')  # 处理中队列
REDIS_SEEN_KEY = CONFIG.get('redis_seen_key', 'hde_seen')  # 已入队项目集合
REDIS_SCAN_PAGE_KEY = CONFIG.get('redis_scan_page_key', 'hde_scan_page')  # 列表扫描断点页码
REDIS_SCAN_COMPLETE_KEY = CONFIG.get('redis_scan_complete_key', 'hde_scan_complete')  # 列表扫描完成标记
REDIS_NEXT_END_TITLES_KEY = CONFIG.get('redis_next_end_titles_key', 'hde_next_end_titles')  # 下一轮截止标题

SIZE_WITH_DASH_PATTERN = re.compile(r"\s[–-]\s*([\d.]+\s*(?:GB|MB|TB))\s*$")
TRAILING_SIZE_PATTERN = re.compile(r"([\d.]+\s*(?:GB|MB|TB))\s*$")
IMAGE_LINK_PATTERN = re.compile(r"\.(?:png|jpe?g|gif|webp|avif)(?:$|[?#])", re.IGNORECASE)
SESSION_PROXIES = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890",
}

requests.packages.urllib3.disable_warnings()


def build_retry_strategy() -> Retry:
    """构造 requests 适配器使用的重试策略。"""
    return Retry(
        total=15,
        status_forcelist=[502],
        allowed_methods=["POST", "GET"],
        backoff_factor=1,
    )


def build_hde_session() -> requests.Session:
    """创建带连接池、重试和代理配置的会话。"""
    adapter = HTTPAdapter(max_retries=build_retry_strategy(), pool_connections=20, pool_maxsize=40)
    request_session = requests.Session()
    request_session.proxies = SESSION_PROXIES.copy()
    request_session.mount("http://", adapter)
    request_session.mount("https://", adapter)
    return request_session


session = build_hde_session()


def normalize_end_titles(titles: Iterable[str]) -> List[str]:
    """清洗配置中的截止标题列表。"""
    return [title.strip() for title in titles if isinstance(title, str) and title.strip()]


def get_current_end_titles() -> List[str]:
    """读取当前生效的截止标题列表。"""
    config = read_json_to_dict(CONFIG_PATH)
    titles = config.get("end_titles") or END_TITLES
    return normalize_end_titles(titles)


def build_hde_page_url(page_number: int) -> str:
    """根据页码构造 HDE 电影列表页 URL。"""
    return f"{HDE_URL}tag/movies/page/{page_number}/"


def should_stop_scrapy(result_list: List[Dict[str, str]], end_titles: List[str]) -> bool:
    """当前批次命中任一截止标题时返回 True。"""
    if not end_titles:
        return False

    return any(result_item.get("title") in end_titles for result_item in result_list)


def select_next_end_titles(result_list: List[Dict[str, str]], keep_count: int = END_TITLES_KEEP_COUNT) -> List[str]:
    """从首次访问页中选出下一轮要写回配置的前几个标题。"""
    titles = [result_item.get("title", "").strip() for result_item in result_list if result_item.get("title", "").strip()]
    return titles[:keep_count]


def serialize_hde_post(result_item: Dict[str, str]) -> str:
    """将列表项序列化为 Redis 队列任务。"""
    return serialize_payload(
        {
            "title": result_item["title"],
            "url": result_item["url"],
            "size": result_item["size"],
        }
    )


def enqueue_hde_posts(start_page: int = 1, redis_client: redis.Redis | None = None) -> None:
    """顺序翻页，收集新帖子并写入 Redis 待处理队列。"""
    if redis_client is None:
        redis_client = get_redis_client()

    end_titles = get_current_end_titles()
    if not end_titles:
        raise ValueError("至少需要提供一个截止标题")

    if redis_client.get(REDIS_SCAN_COMPLETE_KEY) == "1":
        logger.info("HDE 列表扫描已完成，跳过入队阶段")
        return

    saved_page = redis_client.get(REDIS_SCAN_PAGE_KEY)
    current_page = int(saved_page) if saved_page is not None else start_page
    if saved_page is None:
        redis_client.set(REDIS_SCAN_PAGE_KEY, str(current_page))
    else:
        logger.info(f"HDE 从第 {current_page} 页继续扫描")

    while True:
        logger.info(f"抓取第 {current_page} 页")
        response = get_hde_response(build_hde_page_url(current_page))
        result_list = parse_hde_response(response)
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
            unique_value=lambda item: item["url"],
            serializer=serialize_hde_post,
        )
        logger.info(f"第 {current_page} 页解析 {len(result_list)} 条，入队 {enqueued_count} 条")

        if should_stop_scrapy(result_list, end_titles):
            redis_client.set(REDIS_SCAN_COMPLETE_KEY, "1")
            redis_client.set(REDIS_SCAN_PAGE_KEY, str(current_page + 1))
            logger.info("HDE 列表扫描完成")
            break

        current_page += 1
        redis_client.set(REDIS_SCAN_PAGE_KEY, str(current_page))
        logger.warning("-" * 255)


def recover_hde_processing_when_pending_is_empty(redis_client: redis.Redis) -> int:
    """启动时若待处理为空但处理中有残留，则回退到待处理并继续运行。"""
    if redis_client.llen(REDIS_PENDING_KEY) or not redis_client.llen(REDIS_PROCESSING_KEY):
        return 0

    recovered_count = 0
    while True:
        payload = redis_client.rpoplpush(REDIS_PROCESSING_KEY, REDIS_PENDING_KEY)
        if not payload:
            break
        recovered_count += 1

    logger.warning(f"HDE 检测到待处理为空但处理中残留 {recovered_count} 条，已回退到待处理队列并继续运行")
    return recovered_count


def drain_hde_queue(redis_client: redis.Redis | None = None) -> None:
    """从 Redis 队列中取帖子，使用多线程访问详情页并写出 ``.rls`` 文件。"""
    if redis_client is None:
        redis_client = get_redis_client()

    drain_queue(
        redis_client,
        pending_key=REDIS_PENDING_KEY,
        processing_key=REDIS_PROCESSING_KEY,
        max_workers=DEFAULT_MAX_WORKERS,
        worker=visit_hde_url,
        deserialize=deserialize_payload,
        logger=logger,
        queue_label="HDE",
        identify_item=lambda info: info["url"],
        recover_processing_on_start=False,
        keep_failed_in_processing=True,
    )


def finalize_hde_run(redis_client: redis.Redis | None = None) -> None:
    """在列表扫描和详情任务都结束后，回写 ``end_titles`` 并清理运行状态。"""
    if redis_client is None:
        redis_client = get_redis_client()

    if redis_client.get(REDIS_SCAN_COMPLETE_KEY) != "1":
        logger.info("HDE 列表扫描尚未完成，暂不回写 end_titles")
        return

    pending_count = redis_client.llen(REDIS_PENDING_KEY)
    if pending_count:
        logger.info("HDE 队列仍有未完成任务，暂不回写 end_titles")
        return

    processing_count = redis_client.llen(REDIS_PROCESSING_KEY)
    if processing_count:
        logger.warning(f"HDE 待处理已空，但处理中仍有 {processing_count} 条，已保留处理中队列，请直接重跑")
        return

    next_titles_payload = redis_client.get(REDIS_NEXT_END_TITLES_KEY)
    if not next_titles_payload:
        logger.warning("HDE 未记录新的 end_titles，跳过配置更新")
        return

    update_json_config(CONFIG_PATH, "end_titles", deserialize_payload(next_titles_payload)["titles"])
    redis_client.delete(
        REDIS_PENDING_KEY,
        REDIS_PROCESSING_KEY,
        REDIS_SCAN_PAGE_KEY,
        REDIS_SCAN_COMPLETE_KEY,
        REDIS_NEXT_END_TITLES_KEY,
    )


def scrapy_hde(start_page: int = 1) -> None:
    """先翻页入 Redis，再从 Redis 队列中多线程抓取详情。"""
    logger.info("抓取 hde 站点发布信息")
    redis_client = get_redis_client()
    try:
        recover_hde_processing_when_pending_is_empty(redis_client)
        enqueue_hde_posts(start_page=start_page, redis_client=redis_client)
        drain_hde_queue(redis_client=redis_client)
    finally:
        finalize_hde_run(redis_client=redis_client)


@retry(
    stop_max_attempt_number=RETRY_MAX_ATTEMPTS,
    wait_random_min=RETRY_WAIT_MIN_MS,
    wait_random_max=RETRY_WAIT_MAX_MS,
)
def get_hde_response(url: str, session: requests.Session | None = None) -> requests.Response:
    """请求页面并统一做编码设置与状态码校验。"""
    request_client = session or globals()["session"]
    response = request_client.get(url, timeout=REQUEST_TIMEOUT_SECONDS, verify=False)
    response.encoding = 'utf-8'
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    return response


@retry(
    stop_max_attempt_number=RETRY_MAX_ATTEMPTS,
    wait_random_min=RETRY_WAIT_MIN_MS,
    wait_random_max=RETRY_WAIT_MAX_MS,
)
def post_hde_response(url: str, payload: List[tuple[str, str]], session: requests.Session) -> requests.Response:
    """提交内容保护表单并返回解锁后的页面。"""
    response = session.post(
        url,
        data=payload,
        timeout=REQUEST_TIMEOUT_SECONDS,
        verify=False,
    )
    response.encoding = 'utf-8'
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    return response


def parse_hde_response(response: requests.Response) -> list:
    """解析 HDE 单页列表，输出 ``title/url/size`` 字典列表。"""
    soup = BeautifulSoup(response.text, "lxml")
    results = []
    for fit in soup.select("div.fit.item"):
        result_item = parse_hde_item(fit)
        if result_item:
            results.append(result_item)
    return results


def parse_hde_item(fit) -> Dict[str, str] | None:
    """解析单个列表条目，缺少必要节点时返回 ``None``。"""
    data_div = fit.select_one("div.data")
    if not data_div:
        return None

    a_tag = data_div.select_one("h5 a")
    if not a_tag:
        return None

    title = a_tag.get_text(strip=True)
    url = a_tag.get("href", "").strip()
    return {
        "title": title,
        "url": url,
        "size": extract_release_size(title),
    }


def extract_release_size(title: str, default_size: str = DEFAULT_RELEASE_SIZE) -> str:
    """从标题末尾提取体积信息，失败时回退到配置中的默认值。"""
    match = SIZE_WITH_DASH_PATTERN.search(title)
    if not match:
        match = TRAILING_SIZE_PATTERN.search(title)
    if match:
        return match.group(1).replace(" ", "")
    return default_size.replace(" ", "")


def visit_hde_url(result_item: dict):
    """
    访问详情页并写出对应的 ``.rls`` 文件。
    """
    url = result_item["url"]
    logger.info(f"访问 {url}")
    detail_session = build_hde_session()
    response = get_hde_response(url, session=detail_session)
    soup = BeautifulSoup(response.text, 'lxml')
    result_item["imdb"] = extract_imdb_id_from_soup(soup)
    soup = unlock_hde_protected_soup(url, soup, detail_session)
    content = build_hde_output_content(url, result_item["imdb"], soup)
    path = os.path.join(OUTPUT_DIR, build_hde_output_filename(result_item))
    write_list_to_file(path, content)


def find_hde_protected_form(soup: BeautifulSoup):
    """从详情页中定位内容保护表单。"""
    for form in soup.find_all("form"):
        if form.find("input", attrs={"name": "content-protector-token"}) is not None:
            return form
    return None


def build_hde_protected_form_payload(form) -> List[tuple[str, str]]:
    """提取内容保护表单里需要回发的字段。"""
    payload = []
    for input_tag in form.find_all("input"):
        input_type = input_tag.get("type", "").lower()
        name = input_tag.get("name", "").strip()
        if not name:
            continue
        if input_type in {"hidden", "submit"}:
            payload.append((name, input_tag.get("value", "")))
    return payload


def unlock_hde_protected_soup(url: str, soup: BeautifulSoup, session: requests.Session) -> BeautifulSoup:
    """如果页面带内容保护表单，则自动提交表单并返回解锁后的页面。"""
    protected_form = find_hde_protected_form(soup)
    if protected_form is None:
        return soup

    payload = build_hde_protected_form_payload(protected_form)
    if not payload:
        return soup

    response = post_hde_response(url, payload, session)
    return BeautifulSoup(response.text, "lxml")


def build_hde_output_content(url: str, imdb_id: str, soup: BeautifulSoup) -> List[str]:
    """生成写入 ``.rls`` 文件的内容：详情页地址、IMDb 地址和解锁后的下载链接。"""
    useful_links = extract_hde_useful_links(soup)
    if imdb_id:
        useful_links = dedupe_hde_links([f"https://www.imdb.com/title/{imdb_id}/", *useful_links])
    return [url, *useful_links]


def extract_hde_useful_links(soup: BeautifulSoup) -> List[str]:
    """从正文中提取 IMDb 和解锁后的下载链接，过滤截图等无用链接。"""
    entry_content = soup.select_one("div.entry-content") or soup
    links = []
    for anchor in entry_content.find_all("a", href=True):
        href = urljoin(HDE_URL, anchor["href"].strip())
        if is_hde_useful_link(href):
            links.append(href)
    return dedupe_hde_links(links)


def is_hde_useful_link(href: str) -> bool:
    """判断链接是否值得写入输出文件。"""
    if not href:
        return False

    lower_href = href.lower()
    if "/cdn-cgi/l/email-protection" in lower_href:
        return False
    if IMAGE_LINK_PATTERN.search(lower_href) or "pixhost." in lower_href:
        return False
    if "imdb.com/title/" in lower_href or lower_href.startswith("magnet:"):
        return True

    parsed = urlparse(href)
    if parsed.netloc and parsed.netloc not in {"hdencode.org", "www.hdencode.org"}:
        return True
    return False


def dedupe_hde_links(links: Iterable[str]) -> List[str]:
    """按原顺序去重链接列表。"""
    deduped = []
    seen = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        deduped.append(link)
    return deduped


def extract_imdb_id_from_soup(soup: BeautifulSoup) -> str:
    """从详情页所有链接中提取 IMDb 编号。"""
    return extract_imdb_id_from_links(a["href"] for a in soup.find_all("a", href=True))


def extract_imdb_id_from_links(hrefs: Iterable[str]) -> str:
    """
    优先从标准 IMDb 标题页 URL 提取，其次回退到宽松 ``tt`` 编号匹配。
    """
    return extract_shared_imdb_id_from_links(hrefs) or ""


def build_hde_output_filename(result_item: Dict[str, str]) -> str:
    """根据发布信息拼装输出文件名，格式为 ``标题 - hde (体积)[IMDb].rls``。"""
    file_name = normalize_release_title_for_filename(result_item["title"])
    file_name = sanitize_filename(file_name)
    return f"{file_name} - hde ({result_item['size']})[{result_item.get('imdb', '')}].rls"
