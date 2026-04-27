"""
抓取 sk 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import re
from datetime import datetime, timedelta
from typing import cast

import redis
import requests
from bs4 import BeautifulSoup
from retrying import retry

from my_module import (
    normalize_release_title_for_filename,
    read_json_to_dict,
    sanitize_filename,
    update_json_config,
    write_list_to_file,
)
from scrapy_redis import (
    drain_queue,
    get_redis_client,
    push_items_to_queue,
    serialize_payload,
)
from sort_movie_request import get_csfd_response, get_csfd_movie_details

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG_PATH = 'config/scrapy_sk.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

SK_URL = CONFIG['sk_url']  # sk 地址
SK_MOVIE_URL = CONFIG['sk_movie_url']  # sk 电影列表地址
SK_COOKIE = CONFIG['sk_cookie']  # 用户甜甜
REQUEST_HEAD = CONFIG['request_head']  # 请求头
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录
THREAD_NUMBER = CONFIG['thread_number']  # 线程数
END_DATA = CONFIG['end_data']  # 截止日期
MAX_EMPTY_PAGES = CONFIG.get('max_empty_pages', 5)  # 连续空页上限
EXCLUDED_GROUPS = tuple(CONFIG.get('excluded_groups', ['Knihy a Časopisy']))  # 排除分组

REDIS_PENDING_KEY = CONFIG.get('redis_pending_key', 'sk_pending')  # 待处理队列
REDIS_PROCESSING_KEY = CONFIG.get('redis_processing_key', 'sk_processing')  # 处理中队列
REDIS_FAILED_KEY = CONFIG.get('redis_failed_key', 'sk_failed')  # 失败队列
REDIS_SEEN_KEY = CONFIG.get('redis_seen_key', 'sk_seen')  # 已入队项目集合
REDIS_SCAN_PAGE_KEY = CONFIG.get('redis_scan_page_key', 'sk_scan_page')  # 列表扫描断点页码
REDIS_SCAN_COMPLETE_KEY = CONFIG.get('redis_scan_complete_key', 'sk_scan_complete')  # 列表扫描完成标记
REDIS_NEXT_END_DATA_KEY = CONFIG.get('redis_next_end_data_key', 'sk_next_end_data')  # 下一轮截止日期

REQUEST_HEAD["Cookie"] = SK_COOKIE  # 请求头加入认证


def get_previous_day(date_str: str) -> str:
    """返回 ``date_str`` 的前一天，格式保持 ``dd/mm/YYYY``。"""
    date_value = datetime.strptime(date_str, "%d/%m/%Y")
    previous_day = date_value - timedelta(days=1)
    return previous_day.strftime("%d/%m/%Y")


def get_current_end_data() -> str:
    """读取当前配置中的截止日期，避免同进程多次运行时使用过期值。"""
    return read_json_to_dict(CONFIG_PATH).get("end_data", END_DATA)


def fetch_sk_page(page_no: int) -> list[dict]:
    """抓取并解析单个 SK 列表页。"""
    logger.info(f"抓取第 {page_no} 页")
    url = f"{SK_MOVIE_URL}{page_no}"
    response = get_sk_response(url)
    result_list = parse_sk_response(response)
    if not result_list:
        if is_sk_filtered_empty_page(response.text):
            logger.info(f"第 {page_no} 页无有效帖子，疑似被账户设置过滤")
            return []
        if is_sk_excluded_groups_only_page(response.text):
            logger.info(f"第 {page_no} 页仅包含排除分组帖子，已跳过")
            return []
        raise RuntimeError("SK 列表页解析结果为空，网站结构可能已变更")
    logger.info(f"共 {len(result_list)} 个结果")
    return result_list


def get_scan_start_page(redis_client: redis.Redis, start_page: int) -> int:
    """读取 Redis 中保存的扫描页码，或初始化为 ``start_page``。"""
    saved_page = cast(str | None, redis_client.get(REDIS_SCAN_PAGE_KEY))
    current_page = int(saved_page) if saved_page is not None else start_page
    if saved_page is None:
        redis_client.set(REDIS_SCAN_PAGE_KEY, str(current_page))
    else:
        logger.info(f"从第 {current_page} 页继续扫描")
    return current_page


def advance_scan_page(redis_client: redis.Redis, current_page: int) -> int:
    """推进到下一页，并写回 Redis 断点。"""
    next_page = current_page + 1
    redis_client.set(REDIS_SCAN_PAGE_KEY, str(next_page))
    logger.info("-" * 255)
    return next_page


def ensure_next_end_data(redis_client: redis.Redis, result_list: list[dict]) -> None:
    """首次拿到有效帖子时，记录下一轮截止日期。"""
    if redis_client.get(REDIS_NEXT_END_DATA_KEY) is None:
        redis_client.set(REDIS_NEXT_END_DATA_KEY, get_previous_day(result_list[0]["date"]))


def serialize_sk_post(item: dict) -> str:
    """将 SK 列表项序列化为 Redis 队列任务。"""
    return serialize_payload(
        {
            "group": item["group"],
            "url": item["url"],
            "title": item["title"],
            "size": item["size"],
            "date": item["date"],
        }
    )


def enqueue_sk_page_results(redis_client: redis.Redis, result_list: list[dict]) -> int:
    """将单页有效帖子写入 Redis 待处理队列。"""
    ensure_next_end_data(redis_client, result_list)
    return push_items_to_queue(
        redis_client,
        result_list,
        seen_key=REDIS_SEEN_KEY,
        pending_key=REDIS_PENDING_KEY,
        unique_value=lambda item: item["url"],
        serializer=serialize_sk_post,
    )


def mark_scan_complete(redis_client: redis.Redis, current_page: int) -> None:
    """标记列表扫描完成，并保存下一页断点。"""
    redis_client.set(REDIS_SCAN_COMPLETE_KEY, "1")
    redis_client.set(REDIS_SCAN_PAGE_KEY, str(current_page + 1))
    logger.info("SK 列表扫描完成")


def enqueue_sk_posts(start_page: int = 0, end_data: str | None = None, redis_client: redis.Redis | None = None) -> None:
    """顺序翻页，收集帖子并写入 Redis 待处理队列。"""
    if end_data is None:
        end_data = get_current_end_data()
    if redis_client is None:
        redis_client = get_redis_client()

    if redis_client.get(REDIS_SCAN_COMPLETE_KEY) == "1":
        logger.info("SK 列表扫描已完成，跳过入队阶段")
        return

    current_page = get_scan_start_page(redis_client, start_page)

    empty_page_count = 0
    while True:
        result_list = fetch_sk_page(current_page)
        if not result_list:
            empty_page_count += 1
            if empty_page_count >= MAX_EMPTY_PAGES:
                raise RuntimeError(f"SK 连续 {empty_page_count} 页无有效帖子，已停止扫描")

            current_page = advance_scan_page(redis_client, current_page)
            continue

        empty_page_count = 0
        enqueued_count = enqueue_sk_page_results(redis_client, result_list)
        logger.info(f"第 {current_page} 页解析 {len(result_list)} 条，入队 {enqueued_count} 条")

        if any(result_item["date"] == end_data for result_item in result_list):
            mark_scan_complete(redis_client, current_page)
            break

        current_page = advance_scan_page(redis_client, current_page)


def drain_sk_queue(redis_client: redis.Redis | None = None) -> None:
    """从 Redis 队列中取帖子，使用多线程访问详情页并写出 .sk 文件。"""
    if redis_client is None:
        redis_client = get_redis_client()

    drain_queue(
        redis_client,
        pending_key=REDIS_PENDING_KEY,
        processing_key=REDIS_PROCESSING_KEY,
        failed_key=REDIS_FAILED_KEY,
        max_workers=THREAD_NUMBER,
        worker=visit_sk_url,
        logger=logger,
        queue_label="SK",
        identify_item=lambda info: info["url"],
    )


def finalize_sk_run(redis_client: redis.Redis | None = None) -> None:
    """在扫描和详情任务都结束后，回写 end_data 并清理本轮运行状态。"""
    if redis_client is None:
        redis_client = get_redis_client()

    if redis_client.get(REDIS_SCAN_COMPLETE_KEY) != "1":
        logger.info("SK 列表扫描尚未完成，暂不回写 end_data")
        return

    if redis_client.llen(REDIS_PENDING_KEY) or redis_client.llen(REDIS_PROCESSING_KEY):
        logger.info("SK 队列仍有未完成任务，暂不回写 end_data")
        return

    next_end_data = cast(str | None, redis_client.get(REDIS_NEXT_END_DATA_KEY))
    if not next_end_data:
        logger.warning("SK 未记录新的 end_data，跳过配置更新")
        return

    update_json_config(CONFIG_PATH, "end_data", next_end_data)
    redis_client.delete(
        REDIS_PENDING_KEY,
        REDIS_PROCESSING_KEY,
        REDIS_SEEN_KEY,
        REDIS_SCAN_PAGE_KEY,
        REDIS_SCAN_COMPLETE_KEY,
        REDIS_NEXT_END_DATA_KEY,
    )

    failed_count = redis_client.llen(REDIS_FAILED_KEY)
    if failed_count:
        logger.warning(f"SK 队列有 {failed_count} 条失败任务保留在 Redis 中待手动排查")


def scrapy_sk(start_page: int = 0) -> None:
    """
    先翻页入 Redis，再从 Redis 队列中多线程抓取详情。
    """
    logger.info("抓取 sk 站点发布信息")
    redis_client = get_redis_client()
    enqueue_sk_posts(start_page=start_page, redis_client=redis_client)
    drain_sk_queue(redis_client=redis_client)
    finalize_sk_run(redis_client=redis_client)


@retry(stop_max_attempt_number=15, wait_random_min=1000, wait_random_max=10000)
def get_sk_response(url: str) -> requests.Response:
    """请求流程"""
    response = requests.get(url, headers=REQUEST_HEAD, timeout=20)
    response.encoding = 'utf-8'
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    return response


def extract_sk_row_links(td) -> dict | None:
    """从 SK 列表项中提取分组、详情页链接和标题。"""
    link_snapshot = inspect_sk_row_links(td)
    if not (link_snapshot["group"] and link_snapshot["url"] and link_snapshot["title"]):
        return None

    return {
        "group": link_snapshot["group"],
        "url": link_snapshot["url"],
        "title": link_snapshot["title"],
    }


def inspect_sk_row_links(td) -> dict:
    """提取链接字段，并保留调试所需的原始片段。"""
    group = None
    url = None
    title = None

    for a in td.find_all('a', href=True):
        href = a['href']
        if href.startswith("torrents_v2.php?category"):
            group = a.get_text(strip=True)
        elif href.startswith("details.php?name"):
            url = SK_URL + "torrent/" + href
            title = a.get_text(strip=True)

    return {
        "group": group,
        "url": url,
        "title": title,
        "td_snippet": re.sub(r"\s+", " ", str(td)).strip()[:500],
    }


def parse_sk_row(td) -> dict | None:
    """解析单个 SK 列表项。"""
    link_snapshot = inspect_sk_row_links(td)
    if not (link_snapshot["group"] and link_snapshot["url"] and link_snapshot["title"]):
        logger.info(
            "跳过：缺少链接字段 - group=%r url=%r title=%r td=%s",
            link_snapshot["group"],
            link_snapshot["url"],
            link_snapshot["title"],
            link_snapshot["td_snippet"],
        )
        return None
    if is_sk_excluded_group(link_snapshot["group"]):
        return None
    links = {
        "group": link_snapshot["group"],
        "url": link_snapshot["url"],
        "title": link_snapshot["title"],
    }

    size_date = extract_sk_row_size_date(td)
    if not size_date:
        logger.info(f"跳过：缺少大小日期字段 - {links['title']} - {links['url']}")
        return None

    return {
        **links,
        **size_date,
    }


def parse_sk_response(response: requests.Response) -> list:
    """解析流程"""
    if is_sk_filtered_empty_page(response.text):
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    rows = soup.select("table.lista table.lista td.lista")
    if not rows:
        raise RuntimeError("未找到 SK 列表项，网站结构可能已变更")

    results = []
    for td in rows:
        result = parse_sk_row(td)
        if result:
            results.append(result)

    return results


def is_sk_filtered_empty_page(html: str) -> bool:
    """识别账户过滤导致的已知空页提示。"""
    soup = BeautifulSoup(html, "html.parser")
    if soup.select_one('a[href^="details.php?name"]'):
        return False

    page_text = " ".join(soup.stripped_strings)
    return (
            "Nenasli ste co ste hladali" in page_text
            and "Napiste nam to na nastenku" in page_text
    )


def is_sk_excluded_group(group: str) -> bool:
    """判断分组是否应从抓取结果中排除。"""
    return group in EXCLUDED_GROUPS


def is_sk_excluded_groups_only_page(html: str) -> bool:
    """识别仅包含排除分组帖子的页面。"""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table.lista table.lista td.lista")
    detail_row_count = 0

    for td in rows:
        link_snapshot = inspect_sk_row_links(td)
        if not (link_snapshot["group"] and link_snapshot["url"] and link_snapshot["title"]):
            continue

        detail_row_count += 1
        if not is_sk_excluded_group(link_snapshot["group"]):
            return False

    return detail_row_count > 0


def extract_sk_row_size_date(td) -> dict | None:
    """从 SK 列表项中提取大小和日期。"""
    for text in td.stripped_strings:
        if "Velkost" not in text or "Pridany" not in text:
            continue

        match = re.search(r"Velkost\s+(.*?)\s*\|\s*Pridany\s+(.*)", text)
        if not match:
            continue

        size = match.group(1).strip()
        date = match.group(2).strip()
        if size and date:
            return {
                "size": size,
                "date": date,
            }

    return None


def extract_csfd_url_from_sk_detail(detail_html: str) -> str | None:
    """从 SK 详情页 HTML 中提取 CSFD 链接。"""
    soup = BeautifulSoup(detail_html, 'lxml')
    img = soup.select_one('a[itemprop="sameAs"] > img[src="/torrent/images/csfd.png"]')
    if not img or not img.parent:
        return None
    return img.parent.get('href')


def get_normalized_csfd_data(csfd_url: str) -> dict:
    """请求并整理 CSFD 数据，保证返回稳定字段。"""
    response = get_csfd_response(csfd_url)
    csfd_data = get_csfd_movie_details(response) or {}
    csfd_id = csfd_data.get("id")
    if not csfd_id:
        csfd_id = "csfd" + csfd_url.rstrip("/").split("/")[-1]

    return {
        "origin": csfd_data.get("origin", ""),
        "director": csfd_data.get("director", ""),
        "id": csfd_id,
    }


def build_sk_output_filename(result_item: dict, csfd_data: dict) -> str:
    """根据抓取结果和 CSFD 信息生成输出文件名。"""
    file_name = result_item['title'] + "#" + csfd_data['origin'] + "#" + "{" + csfd_data['director'] + "}"
    file_name = normalize_release_title_for_filename(
        file_name,
        extra_cleanup_patterns=(r'\s*=\s*CSFD\s*\d+%',),
    )
    file_name = sanitize_filename(file_name) + "(" + result_item['size'] + ")" + "[" + csfd_data["id"] + "]"
    return f"{file_name}.sk"


def visit_sk_url(result_item: dict):
    """访问详情页"""
    url = result_item["url"]
    logger.info(f"访问 {url}")
    response = get_sk_response(url)
    csfd_url = extract_csfd_url_from_sk_detail(response.text)
    if not csfd_url:
        raise RuntimeError(f"未找到 CSFD 链接：{url}")

    csfd_data = get_normalized_csfd_data(csfd_url)
    file_name = build_sk_output_filename(result_item, csfd_data)
    path = os.path.join(OUTPUT_DIR, file_name)
    write_list_to_file(path, [url])
