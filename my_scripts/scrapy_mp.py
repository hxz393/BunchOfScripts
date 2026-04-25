"""
抓取 mp 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import re

import redis
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from retrying import retry

from my_module import (
    normalize_release_title_for_filename,
    read_json_to_dict,
    sanitize_filename,
    write_list_to_file,
)
from scrapy_redis import (
    deserialize_payload,
    drain_queue,
    get_redis_client,
    serialize_payload,
)

logger = logging.getLogger(__name__)

CONFIG_PATH = 'config/scrapy_mp.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

MP_MOVIE_URL = CONFIG['mp_movie_url']  # mp 电影列表地址
MP_VERIFICATION_URL = CONFIG.get('mp_verification_url')  # 手动过 CF 的验证入口页
MP_COOKIE = CONFIG['mp_cookie']  # 用户甜甜
REQUEST_HEAD = CONFIG['request_head']  # 请求头
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录
THREAD_NUMBER = CONFIG.get('thread_number', 35)  # 线程数

REDIS_PENDING_KEY = CONFIG.get('redis_pending_key', 'mp_pending')  # 待处理队列
REDIS_PROCESSING_KEY = CONFIG.get('redis_processing_key', 'mp_processing')  # 处理中队列
REDIS_SCAN_PAGE_KEY = CONFIG.get('redis_scan_page_key', 'mp_scan_page')  # 列表扫描断点页码
REDIS_SCAN_COMPLETE_KEY = CONFIG.get('redis_scan_complete_key', 'mp_scan_complete')  # 列表扫描完成标记

REQUEST_HEAD["Cookie"] = MP_COOKIE  # 请求头加入认证


class MpCloudflareError(RuntimeError):
    """MP 请求命中 Cloudflare 验证页，通常意味着 Cookie 已失效。"""


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


def get_mp_queued_links(redis_client: redis.Redis) -> set[str]:
    """读取当前 Redis 队列中已经存在的帖子 URL。"""
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


def request_mp_page(url: str, *, allow_not_found: bool = False) -> requests.Response:
    """请求 MP 页面；命中 Cloudflare 时直接停止，等待人工处理后重跑。"""
    headers = dict(REQUEST_HEAD)
    headers["Cookie"] = MP_COOKIE
    response = requests.get(url, headers=headers, timeout=20)
    response.encoding = 'utf-8'
    if is_mp_cloudflare_challenge(response):
        verification_url = MP_VERIFICATION_URL or url
        raise MpCloudflareError(
            f"mp Cookie 已失效或触发 Cloudflare 验证，请先在浏览器手动通过后重跑：{verification_url}"
        )

    if response.status_code == 404 and allow_not_found:
        return response
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")
    return response


def validate_mp_end_urls(end_urls: set[str]) -> None:
    """运行前校验截止 URL 是否仍然可访问，避免因 404 导致无限翻页。"""
    logger.info("校验阶段...")
    missing_urls = []
    for url in sorted(end_urls):
        response = request_mp_page(url, allow_not_found=True)
        if response.status_code == 404:
            missing_urls.append(url)

    if missing_urls:
        raise ValueError(f"mp 截止 URL 已失效（404）：{', '.join(missing_urls)}")


def scrapy_mp(start_page, end) -> None:
    """
    先顺序翻页把新帖子写入 Redis，再并发抓取详情页。
    """
    logger.info("抓取 mp 站点发布信息")
    redis_client = get_redis_client()
    try:
        recover_mp_processing_when_pending_is_empty(redis_client)
        enqueue_mp_posts(start_page=start_page, end=end, redis_client=redis_client)
        drain_mp_queue(redis_client=redis_client)
    finally:
        finalize_mp_run(redis_client=redis_client)


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

    saved_page = redis_client.get(REDIS_SCAN_PAGE_KEY)
    current_page = int(saved_page) if saved_page is not None else start_page
    if saved_page is None:
        redis_client.set(REDIS_SCAN_PAGE_KEY, str(current_page))
    else:
        logger.info(f"从第 {current_page} 页继续扫描")

    queued_links = get_mp_queued_links(redis_client)
    matched_end_urls = queued_links & end_urls
    while True:
        logger.info(f"抓取第 {current_page} 页")
        url = f"{MP_MOVIE_URL}{current_page}/"
        response = get_mp_response(url)
        result_list = parse_mp_response(response)
        if not result_list:
            raise RuntimeError("MP 列表页解析结果为空，网站结构可能已变更")

        page_unique_links = set()
        new_items = []
        for item in result_list:
            link = item["link"]
            if not link or link in queued_links or link in page_unique_links:
                continue
            page_unique_links.add(link)
            new_items.append(item)

        if new_items:
            pipe = redis_client.pipeline()
            for item in new_items:
                pipe.rpush(REDIS_PENDING_KEY, serialize_mp_post(item))
            pipe.execute()

        enqueued_count = len(new_items)
        skipped_count = len(result_list) - enqueued_count
        queued_links.update(page_unique_links)
        matched_end_urls.update(item["link"] for item in result_list if item["link"] in end_urls)
        logger.info(
            f"第 {current_page} 页解析 {len(result_list)} 条，入队 {enqueued_count} 条，跳过重复 {skipped_count} 条，"
            f"截止 URL 已命中 {len(matched_end_urls)}/{len(end_urls)} 条"
        )

        if matched_end_urls == end_urls:
            redis_client.set(REDIS_SCAN_COMPLETE_KEY, "1")
            redis_client.set(REDIS_SCAN_PAGE_KEY, str(current_page + 1))
            logger.info("MP 列表扫描完成")
            break

        current_page += 1
        redis_client.set(REDIS_SCAN_PAGE_KEY, str(current_page))
        logger.info("-" * 80)


def recover_mp_processing_queue(redis_client: redis.Redis) -> int:
    """将处理中队列中的残留任务恢复回待处理队列，留待下次重跑。"""
    recovered_count = 0
    while True:
        payload = redis_client.rpoplpush(REDIS_PROCESSING_KEY, REDIS_PENDING_KEY)
        if not payload:
            break
        recovered_count += 1

    return recovered_count


def recover_mp_processing_when_pending_is_empty(redis_client: redis.Redis) -> int:
    """启动时若待处理为空但处理中有残留，则回退到待处理并继续运行。"""
    if redis_client.llen(REDIS_PENDING_KEY) or not redis_client.llen(REDIS_PROCESSING_KEY):
        return 0

    recovered_count = recover_mp_processing_queue(redis_client)
    logger.warning(f"MP 检测到待处理为空但处理中残留 {recovered_count} 条，已回退到待处理队列并继续运行")
    return recovered_count


def drain_mp_queue(redis_client: redis.Redis | None = None) -> None:
    """从 Redis 队列中取帖子，使用多线程访问详情页并写出 .rare 文件。"""
    if redis_client is None:
        redis_client = get_redis_client()

    drain_queue(
        redis_client,
        pending_key=REDIS_PENDING_KEY,
        processing_key=REDIS_PROCESSING_KEY,
        max_workers=THREAD_NUMBER,
        worker=visit_mp_url,
        deserialize=deserialize_payload,
        logger=logger,
        queue_label="MP",
        identify_item=lambda info: info["link"],
        abort_on_exception=lambda exc: isinstance(exc, MpCloudflareError),
        recover_processing_on_start=False,
        keep_failed_in_processing=True,
    )


def finalize_mp_run(redis_client: redis.Redis | None = None) -> None:
    """在扫描和详情任务都结束后，清理本轮运行的扫描状态。"""
    if redis_client is None:
        redis_client = get_redis_client()

    if redis_client.get(REDIS_SCAN_COMPLETE_KEY) != "1":
        logger.info("MP 列表扫描尚未完成，暂不清理扫描状态")
        return

    pending_count = redis_client.llen(REDIS_PENDING_KEY)
    if pending_count:
        logger.info("MP 待处理队列仍有未完成任务，暂不清理扫描状态")
        return

    processing_count = redis_client.llen(REDIS_PROCESSING_KEY)
    if processing_count:
        logger.warning(
            f"MP 待处理已空，但处理中仍有 {processing_count} 条，已保留处理中队列，请直接重跑"
        )
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
    return request_mp_page(url)


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
        raise ValueError(f"MP 详情页缺少 custom_fields2：{result_item['link']}")

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
        raise ValueError(f"MP 详情页缺少 description：{result_item['link']}")

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
        raise TypeError(f"MP 详情解析结果类型无效：{url}")

    path = os.path.join(OUTPUT_DIR, result_dict['file_name'])
    write_list_to_file(path, [url, result_dict['content']])
