"""
抓取 mp 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import re
import threading
import time

import redis
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from retrying import retry

from my_module import (
    normalize_release_title_for_filename,
    read_json_to_dict,
    sanitize_filename,
    update_json_config,
    write_list_to_file,
)
from scrapy_redis import (
    deserialize_payload,
    drain_queue,
    get_redis_client,
    push_items_to_queue,
    serialize_payload,
)

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG_PATH = 'config/scrapy_mp.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

MP_URL = CONFIG['mp_url']  # mp 地址
MP_MOVIE_URL = CONFIG['mp_movie_url']  # mp 电影列表地址
MP_COOKIE = CONFIG['mp_cookie']  # 用户甜甜
REQUEST_HEAD = CONFIG['request_head']  # 请求头
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录
THREAD_NUMBER = CONFIG.get('thread_number', 30)  # 线程数
MP_BROWSER_PROFILE_DIR = CONFIG.get(
    'mp_browser_profile_dir',
    os.path.join(os.path.expanduser("~"), ".bunch_of_scripts", "mp_browser_profile"),
)

REDIS_PENDING_KEY = CONFIG.get('redis_pending_key', 'mp_pending')  # 待处理队列
REDIS_PROCESSING_KEY = CONFIG.get('redis_processing_key', 'mp_processing')  # 处理中队列
REDIS_FAILED_KEY = CONFIG.get('redis_failed_key', 'mp_failed')  # 失败队列
REDIS_SEEN_KEY = CONFIG.get('redis_seen_key', 'mp_seen')  # 已入队帖子集合
REDIS_SCAN_PAGE_KEY = CONFIG.get('redis_scan_page_key', 'mp_scan_page')  # 列表扫描断点页码
REDIS_SCAN_COMPLETE_KEY = CONFIG.get('redis_scan_complete_key', 'mp_scan_complete')  # 列表扫描完成标记

REQUEST_HEAD["Cookie"] = MP_COOKIE  # 请求头加入认证

_mp_cookie_refresh_lock = threading.Lock()
_mp_cookie_last_refresh_time = 0.0
_mp_cookie_refresh_ttl = 30


class MpCloudflareError(RuntimeError):
    """MP 请求命中 Cloudflare 验证页，通常意味着 Cookie 已失效。"""


def get_mp_playwright():
    """懒加载 Playwright，避免模块导入阶段就强依赖。"""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - 取决于运行环境
        raise RuntimeError("未安装 playwright，无法自动刷新 mp Cookie") from exc
    return sync_playwright


def normalize_mp_end_urls(end) -> set[str]:
    """将截止条件统一整理为 URL 集合。"""
    if end is None:
        return set()
    if isinstance(end, str):
        end_urls = {end}
    else:
        end_urls = {item for item in end if item}
    return {item.strip() for item in end_urls if item.strip()}


def serialize_mp_post(item: dict) -> str:
    """将 MP 列表项序列化为 Redis 队列任务。"""
    return serialize_payload(
        {
            "title": item["title"],
            "link": item["link"],
            "year": item["year"],
        }
    )


def get_mp_active_queue_links(redis_client: redis.Redis) -> set[str]:
    """读取 Redis 活跃队列中的帖子 URL。"""
    queue_links = set()
    for key in (REDIS_PENDING_KEY, REDIS_PROCESSING_KEY):
        for payload in redis_client.lrange(key, 0, -1):
            info = deserialize_payload(payload)
            link = info.get("link")
            if link:
                queue_links.add(link)
    return queue_links


def is_mp_cloudflare_challenge(response: requests.Response) -> bool:
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


def is_mp_cloudflare_html(text: str) -> bool:
    """按页面 HTML 判断是否仍停留在 Cloudflare 验证页。"""
    html = text.lower()
    markers = (
        "<title>just a moment",
        "challenges.cloudflare.com",
        "cf_chl",
        "challenge-platform",
        "cf-browser-verification",
        "<title>attention required!",
    )
    return any(marker in html for marker in markers)


def get_mp_response_once(url: str) -> requests.Response:
    """发起一次 MP 请求并统一设置编码。"""
    response = requests.get(url, headers=REQUEST_HEAD, timeout=20)
    response.encoding = 'utf-8'
    return response


def set_mp_cookie(cookie_str: str, *, persist: bool = False) -> str:
    """更新内存中的 MP Cookie，并按需回写配置文件。"""
    global MP_COOKIE, _mp_cookie_last_refresh_time
    MP_COOKIE = cookie_str
    REQUEST_HEAD["Cookie"] = cookie_str
    _mp_cookie_last_refresh_time = time.time()
    if persist:
        update_json_config(CONFIG_PATH, "mp_cookie", cookie_str)
    return cookie_str


def build_mp_cookie_string(cookies: list[dict]) -> str:
    """将指定域名的 Cookie 列表整理为请求头字符串。"""
    filtered = [
        cookie for cookie in cookies
        if "movieparadise.org" in cookie.get("domain", "")
    ]
    return "; ".join(f"{cookie['name']}={cookie['value']}" for cookie in filtered)


def validate_mp_cookie_candidate(url: str, cookie_str: str) -> requests.Response:
    """用候选 Cookie 反向验证当前 URL 是否已可正常访问。"""
    headers = dict(REQUEST_HEAD)
    headers["Cookie"] = cookie_str
    response = requests.get(url, headers=headers, timeout=20)
    response.encoding = 'utf-8'
    return response


def refresh_mp_cookie_interactively(
        url: str,
        *,
        previous_cookie: str | None = None,
        timeout_seconds: int = 300,
        playwright_factory=None,
) -> str:
    """打开浏览器等待人工过 Cloudflare，成功后自动刷新 MP Cookie。"""
    current_cookie = REQUEST_HEAD.get("Cookie", "")
    if previous_cookie is None:
        previous_cookie = current_cookie

    with _mp_cookie_refresh_lock:
        latest_cookie = REQUEST_HEAD.get("Cookie", "")
        if latest_cookie and latest_cookie != previous_cookie:
            logger.info("检测到 MP Cookie 已被其他线程刷新，继续使用最新 Cookie")
            return latest_cookie
        if latest_cookie and (time.time() - _mp_cookie_last_refresh_time) < _mp_cookie_refresh_ttl:
            logger.info("最近刚刷新过 MP Cookie，继续复用")
            return latest_cookie

        if playwright_factory is None:
            playwright_factory = get_mp_playwright()

        logger.warning(f"检测到 MP Cloudflare 验证，正在打开浏览器，请手动完成验证：{url}")
        with playwright_factory() as p:
            os.makedirs(MP_BROWSER_PROFILE_DIR, exist_ok=True)
            context = p.chromium.launch_persistent_context(
                user_data_dir=MP_BROWSER_PROFILE_DIR,
                headless=False,
                channel="chrome",
                viewport={"width": 1280, "height": 800},
                ignore_default_args=["--enable-automation"],
                args=["--disable-blink-features=AutomationControlled"],
            )
            try:
                page = context.pages[0] if context.pages else context.new_page()
                page.goto(MP_URL, wait_until="domcontentloaded", timeout=60000)
                logger.warning("浏览器已打开。请在首页中手动通过 Cloudflare 验证，脚本会在成功后自动继续。")

                deadline = time.time() + timeout_seconds
                while time.time() < deadline:
                    page.wait_for_timeout(3000)
                    cookies = context.cookies()
                    cookie_names = {cookie["name"] for cookie in cookies if "movieparadise.org" in cookie.get("domain", "")}
                    if "cf_clearance" in cookie_names:
                        cookie_str = build_mp_cookie_string(cookies)
                        if cookie_str:
                            validation_response = validate_mp_cookie_candidate(url, cookie_str)
                            if not is_mp_cloudflare_challenge(validation_response) and validation_response.status_code in (200, 404):
                                set_mp_cookie(cookie_str, persist=True)
                                logger.warning("MP Cookie 已自动更新，继续执行当前任务")
                                return cookie_str

                raise MpCloudflareError("MP Cloudflare 验证超时，请重试并在浏览器中完成验证")
            finally:
                context.close()


def request_mp_page(url: str, *, allow_not_found: bool = False) -> requests.Response:
    """请求 MP 页面，必要时自动刷新 Cookie 后重试一次。"""
    response = get_mp_response_once(url)
    if is_mp_cloudflare_challenge(response):
        previous_cookie = REQUEST_HEAD.get("Cookie", "")
        refresh_mp_cookie_interactively(url, previous_cookie=previous_cookie)
        response = get_mp_response_once(url)

    if is_mp_cloudflare_challenge(response):
        raise MpCloudflareError(f"mp Cookie 已失效或触发 Cloudflare 验证，请先手动过验证并更新 Cookie：{url}")

    if response.status_code == 404 and allow_not_found:
        return response
    return response


def raise_for_mp_response(response: requests.Response, url: str) -> None:
    """校验 MP 响应状态，并识别 Cookie 失效导致的 CF 验证页。"""
    if is_mp_cloudflare_challenge(response):
        raise MpCloudflareError(f"mp Cookie 已失效或触发 Cloudflare 验证，请先手动过验证并更新 Cookie：{url}")
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")


def validate_mp_end_urls(end_urls: set[str]) -> None:
    """运行前校验截止 URL 是否仍然可访问，避免因 404 导致无限翻页。"""
    logger.info(f"校验阶段...")
    missing_urls = []
    for url in sorted(end_urls):
        response = request_mp_page(url, allow_not_found=True)
        if response.status_code == 404:
            missing_urls.append(url)
            continue
        if response.status_code != 200:
            raise RuntimeError(f"验证 mp 截止 URL 失败，状态码 {response.status_code}：{url}")

    if missing_urls:
        raise ValueError(f"mp 截止 URL 已失效（404）：{', '.join(missing_urls)}")


def get_mp_scan_start_page(redis_client: redis.Redis, start_page: int) -> int:
    """读取 Redis 中保存的扫描页码，或初始化为 ``start_page``。"""
    saved_page = redis_client.get(REDIS_SCAN_PAGE_KEY)
    current_page = int(saved_page) if saved_page is not None else start_page
    if saved_page is None:
        redis_client.set(REDIS_SCAN_PAGE_KEY, str(current_page))
    else:
        logger.info(f"从第 {current_page} 页继续扫描")
    return current_page


def advance_mp_scan_page(redis_client: redis.Redis, current_page: int) -> int:
    """推进到下一页，并写回 Redis 断点。"""
    next_page = current_page + 1
    redis_client.set(REDIS_SCAN_PAGE_KEY, str(next_page))
    logger.warning("-" * 255)
    return next_page


def mark_mp_scan_complete(redis_client: redis.Redis, current_page: int) -> None:
    """标记列表扫描完成。"""
    redis_client.set(REDIS_SCAN_COMPLETE_KEY, "1")
    redis_client.set(REDIS_SCAN_PAGE_KEY, str(current_page + 1))
    logger.info("MP 列表扫描完成")


def scrapy_mp(start_page, end) -> None:
    """
    先顺序翻页把新帖子写入 Redis，再并发抓取详情页。
    """
    logger.info("抓取 mp 站点发布信息")
    redis_client = get_redis_client()
    enqueue_mp_posts(start_page=start_page, end=end, redis_client=redis_client)
    drain_mp_queue(redis_client=redis_client)
    finalize_mp_run(redis_client=redis_client)


def process_all(result_list, redis_client: redis.Redis | None = None) -> int:
    """将单页列表结果写入 Redis 待处理队列。"""
    if redis_client is None:
        redis_client = get_redis_client()

    return push_items_to_queue(
        redis_client,
        result_list,
        seen_key=REDIS_SEEN_KEY,
        pending_key=REDIS_PENDING_KEY,
        unique_value=lambda item: item["link"],
        serializer=serialize_mp_post,
    )


def enqueue_mp_posts(start_page: int = 0, end=None, redis_client: redis.Redis | None = None) -> None:
    """顺序翻页，收集新帖子并写入 Redis 待处理队列。"""
    if redis_client is None:
        redis_client = get_redis_client()
    if redis_client.get(REDIS_SCAN_COMPLETE_KEY) == "1":
        logger.info("MP 列表扫描已完成，跳过入队阶段")
        return

    end_urls = normalize_mp_end_urls(end)
    if not end_urls:
        raise ValueError("mp 截止 URL 不能为空")
    validate_mp_end_urls(end_urls)

    current_page = get_mp_scan_start_page(redis_client, start_page)
    matched_end_urls = get_mp_active_queue_links(redis_client) & end_urls
    while True:
        logger.info(f"抓取第 {current_page} 页")
        url = f"{MP_MOVIE_URL}{current_page}/"
        response = get_mp_response(url)
        result_list = parse_mp_response(response)
        if not result_list:
            raise RuntimeError("MP 列表页解析结果为空，网站结构可能已变更")

        enqueued_count = process_all(result_list, redis_client=redis_client)
        matched_end_urls.update(item["link"] for item in result_list if item["link"] in end_urls)
        logger.info(
            f"第 {current_page} 页解析 {len(result_list)} 条，入队 {enqueued_count} 条，"
            f"截止 URL 已命中 {len(matched_end_urls)}/{len(end_urls)} 条"
        )

        if matched_end_urls == end_urls:
            mark_mp_scan_complete(redis_client, current_page)
            break

        current_page = advance_mp_scan_page(redis_client, current_page)


def recover_mp_failed_queue(redis_client: redis.Redis) -> int:
    """将失败队列中的任务恢复回待处理队列，便于更新 Cookie 后重试。"""
    recovered_count = 0
    while True:
        payload = redis_client.rpoplpush(REDIS_FAILED_KEY, REDIS_PENDING_KEY)
        if not payload:
            break
        recovered_count += 1

    if recovered_count:
        logger.warning(f"恢复 {recovered_count} 条失败的 MP 任务回待处理队列")

    return recovered_count


def drain_mp_queue(redis_client: redis.Redis | None = None) -> None:
    """从 Redis 队列中取帖子，使用多线程访问详情页并写出 .rare 文件。"""
    if redis_client is None:
        redis_client = get_redis_client()
    recover_mp_failed_queue(redis_client)

    drain_queue(
    redis_client,
    pending_key=REDIS_PENDING_KEY,
    processing_key=REDIS_PROCESSING_KEY,
    failed_key=REDIS_FAILED_KEY,
    max_workers=THREAD_NUMBER,
        worker=visit_mp_url,
        deserialize=deserialize_payload,
        logger=logger,
        queue_label="MP",
        identify_item=lambda info: info["link"],
        abort_on_exception=lambda exc: isinstance(exc, MpCloudflareError),
    )


def finalize_mp_run(redis_client: redis.Redis | None = None) -> None:
    """在扫描和详情任务都结束后，清理本轮运行的扫描状态。"""
    if redis_client is None:
        redis_client = get_redis_client()

    if redis_client.get(REDIS_SCAN_COMPLETE_KEY) != "1":
        logger.info("MP 列表扫描尚未完成，暂不清理扫描状态")
        return

    if (
            redis_client.llen(REDIS_PENDING_KEY)
            or redis_client.llen(REDIS_PROCESSING_KEY)
            or redis_client.llen(REDIS_FAILED_KEY)
    ):
        logger.info("MP 队列仍有未完成任务，暂不清理扫描状态")
        return

    redis_client.delete(REDIS_SCAN_PAGE_KEY, REDIS_SCAN_COMPLETE_KEY)


def should_retry_mp_request(exc: Exception) -> bool:
    """Cloudflare 验证页属于致命状态，不做无意义重试。"""
    return not isinstance(exc, MpCloudflareError)


@retry(
    stop_max_attempt_number=15,
    wait_random_min=1000,
    wait_random_max=10000,
    retry_on_exception=should_retry_mp_request,
)
def get_mp_response(url: str) -> requests.Response:
    """请求流程"""
    response = request_mp_page(url)
    raise_for_mp_response(response, url)
    return response


def parse_mp_response(response: requests.Response) -> list:
    """解析流程"""
    soup = BeautifulSoup(response.text, 'html.parser')
    container = soup.find('div', id='archive-content')
    results = []
    if not container:
        return results

    for art in container.find_all('article', class_='item movies'):
        result_item = parse_mp_article(art)
        if result_item is not None:
            results.append(result_item)

    return results


def parse_mp_article(article) -> dict | None:
    """解析单个列表页条目。"""
    data_div = article.find('div', class_='data')
    if not data_div:
        logger.warning("mp 列表条目缺少 data 容器，已跳过")
        return None

    h3 = data_div.find('h3')
    if not h3:
        logger.warning("mp 列表条目缺少 h3 标题节点，已跳过")
        return None

    h3_a = h3.find('a', href=True)
    if not h3_a:
        logger.warning("mp 列表条目缺少标题链接，已跳过")
        return None
    title = h3_a.get_text(strip=True)
    link = h3_a['href']

    span = data_div.find('span')
    year = ''
    if span:
        text = span.get_text(strip=True)
        # 圆整年份，支持如 "Jul. 20, 1990" 或 "1990"
        match = re.search(r'\b(19|20)\d{2}\b', text)
        if match:
            year = match.group(0)
    if not year:
        logger.warning(f"mp 列表条目缺少年份：{title}")

    return {
        'title': title,
        'link': link,
        'year': year
    }


def is_mp_image_reference_line(line: str) -> bool:
    """判断一行是否为图片引用地址。"""
    stripped = line.strip()
    if not stripped:
        return False

    return bool(
        re.fullmatch(r'https?://\S+\.(?:jpg|jpeg|png|webp|gif|bmp)(?:\?\S*)?', stripped, re.IGNORECASE)
        or re.search(r'\(https?://[^\s)]+\.(?:jpg|jpeg|png|webp|gif|bmp)(?:\?\S*)?\)$', stripped, re.IGNORECASE)
    )


def is_mp_tmdb_image_line(line: str) -> bool:
    """判断一行是否为 TMDb 图片地址。"""
    return bool(
        re.fullmatch(
            r'https?://image\.tmdb\.org/\S+\.(?:jpg|jpeg|png|webp|gif|bmp)(?:\?\S*)?',
            line.strip(),
            re.IGNORECASE,
        )
    )


def is_mp_screenshot_heading(line: str) -> bool:
    """判断一行是否为截图段标题。"""
    return bool(re.match(r'(?i)^screenshots?\b', line.strip()))


def is_mp_screenshot_detail_line(line: str) -> bool:
    """判断一行是否为截图段内部的图片引用。"""
    stripped = line.strip()
    return stripped.startswith("#") or is_mp_image_reference_line(stripped)


def normalize_mp_size(size_text: str) -> str:
    """统一大小字段中的空白。"""
    return re.sub(r'\s+', ' ', size_text).strip()


def extract_mp_size_candidate(line: str):
    """从单行文本中提取可用于补全 Release 的大小。"""
    stripped = line.strip()
    lower = stripped.lower()
    size_match = re.search(r'\b(\d+(?:\.\d+)?)\s*((?:M|G|T|P)i?B)\b', stripped, re.IGNORECASE)
    if not size_match:
        return None

    size_text = normalize_mp_size(f"{size_match.group(1)} {size_match.group(2)}")
    if lower.startswith("general"):
        return "general", size_text
    if lower.startswith("length"):
        return "length", size_text
    if re.match(r'^\s*size\b', stripped, re.IGNORECASE):
        return "size", size_text
    if lower.startswith("file size"):
        return "file size", size_text
    if re.match(r'^\s*rapidgator(?:\s*#\d+)?\s*~\s*\d+(?:\.\d+)?\s*(?:M|G|T|P)i?B\b', stripped, re.IGNORECASE):
        return "rapidgator", size_text
    return None


def fill_mp_release_sizes(lines: list[str]) -> list[str]:
    """为缺少大小的 Release 行补全大小。"""
    release_lines = list(lines)
    source_priority = {
        "general": 0,
        "length": 1,
        "size": 2,
        "file size": 3,
        "rapidgator": 4,
    }

    for index, line in enumerate(release_lines):
        if not line.startswith("Release:"):
            continue
        if re.search(r'~\s*\d+(?:\.\d+)?\s*(?:M|G|T|P)i?B\b', line, re.IGNORECASE):
            continue

        best_candidate = None
        next_index = index + 1
        while next_index < len(release_lines) and not release_lines[next_index].startswith("Release:"):
            candidate = extract_mp_size_candidate(release_lines[next_index])
            if candidate:
                source_name, size_text = candidate
                if best_candidate is None or source_priority[source_name] < source_priority[best_candidate[0]]:
                    best_candidate = (source_name, size_text)
                    if source_name == "general":
                        break
            next_index += 1

        if best_candidate:
            release_lines[index] = f"{line.rstrip()} ~ {best_candidate[1]}"

    return release_lines


def format_mp_text(text: str) -> str:
    """整理正文文本，增强可读性。"""
    lines = text.splitlines()
    cleaned_lines = []
    in_screenshot_section = False

    for line in lines:
        stripped = line.strip()
        if is_mp_tmdb_image_line(stripped):
            continue
        if in_screenshot_section:
            if not stripped:
                continue
            if is_mp_screenshot_detail_line(stripped):
                continue
            in_screenshot_section = False

        if not stripped:
            cleaned_lines.append("")
            continue
        if is_mp_screenshot_heading(stripped):
            in_screenshot_section = True
            continue
        cleaned_lines.append(stripped)

    cleaned_lines = fill_mp_release_sizes(cleaned_lines)
    formatted_lines = []

    for line in cleaned_lines:
        if line.startswith("Release:"):
            while formatted_lines and formatted_lines[-1] == "":
                formatted_lines.pop()
            if formatted_lines:
                formatted_lines.extend(["", ""])
        formatted_lines.append(line)

    return "\n".join(formatted_lines)


def parse_mp_detail(response: requests.Response, result_item: dict):
    """解析详情页，返回输出文件名和正文内容。"""
    soup = BeautifulSoup(response.text, "html.parser")
    # 提取编号
    cf = soup.find('div', class_='custom_fields2')
    if not cf:
        logger.error("没有找到 IMDB 段落")
        return

    m_id = ""
    for a in cf.find_all('a', href=True):
        href = a['href']
        # 查找 IMDb 和 TMDB ID
        imdb_match = re.search(r'(tt\d+)', href, re.IGNORECASE)
        tmdb_match = re.search(r'themoviedb\.org/movie/(\d+)', href, re.IGNORECASE)
        if imdb_match:
            m_id = imdb_match.group(1)
            break
        elif tmdb_match:
            m_id = f"tmdb{tmdb_match.group(1)}"
            break
    # 提取内容
    desc = soup.find('div', itemprop='description', class_='wp-content')
    if not isinstance(desc, Tag):
        return ""

    # 将 <a> 标签替换为 "文本 (URL)"
    for a in desc.find_all('a', href=True):
        text = a.get_text(strip=True)
        href = a['href']
        replacement = f"{text} ({href})" if text else href
        a.replace_with(replacement)

    # 获取纯文本，保持标签间适当空格/换行
    text = "\n".join(desc.stripped_strings)
    text = format_mp_text(text)
    # 文件名
    file_name = normalize_release_title_for_filename(result_item['title'])
    file_name = sanitize_filename(file_name)
    file_name = f"{file_name}({result_item['year']}) - mpvd [{m_id}].rare"
    return {"file_name": file_name, "content": text}


def visit_mp_url(result_item: dict):
    """访问详情页"""
    url = result_item["link"]
    logger.info(f"访问 {url}")
    response = get_mp_response(url)
    result_dict = parse_mp_detail(response, result_item)
    if not isinstance(result_dict, dict):
        return result_dict

    path = os.path.join(OUTPUT_DIR, result_dict['file_name'])
    write_list_to_file(path, [url, result_dict['content']])
