"""
抓取 hde 站点发布信息

配置文件 ``config/scrapy_hde.json`` 需要提供：
- ``hde_url``: 站点根地址。
- ``output_dir``: 生成 ``.rls`` 文件的输出目录。
- ``default_end_title``: 分页抓取时用于判断“已经追到旧数据”的截止标题。
- ``max_workers``: 详情页并发抓取线程数。
- ``default_release_size``: 标题里提取不到体积信息时使用的默认值。
- ``request_timeout_seconds``: 单次 HTTP 请求超时秒数。
- ``retry_max_attempts`` / ``retry_wait_min_ms`` / ``retry_wait_max_ms``:
  ``get_hde_response`` 的重试参数。

主流程：
1. 抓取列表页并解析出标题、详情页链接和体积。
2. 并发访问详情页，提取 IMDb 编号并落盘为 ``.rls`` 文件。
3. 当列表页中出现配置的截止标题时停止翻页。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List

import requests
from bs4 import BeautifulSoup
from retrying import retry

from my_module import normalize_release_title_for_filename, read_json_to_dict, sanitize_filename, write_list_to_file

logger = logging.getLogger(__name__)

CONFIG_PATH = 'config/scrapy_hde.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

HDE_URL = CONFIG['hde_url']  # hde 地址
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录
DEFAULT_END_TITLE = CONFIG['default_end_title']
DEFAULT_MAX_WORKERS = CONFIG['max_workers']
DEFAULT_RELEASE_SIZE = CONFIG['default_release_size']
REQUEST_TIMEOUT_SECONDS = CONFIG['request_timeout_seconds']
RETRY_MAX_ATTEMPTS = CONFIG['retry_max_attempts']
RETRY_WAIT_MIN_MS = CONFIG['retry_wait_min_ms']
RETRY_WAIT_MAX_MS = CONFIG['retry_wait_max_ms']

SIZE_WITH_DASH_PATTERN = re.compile(r"\s[–-]\s*([\d.]+\s*(?:GB|MB|TB))\s*$")
TRAILING_SIZE_PATTERN = re.compile(r"([\d.]+\s*(?:GB|MB|TB))\s*$")
IMDB_URL_PATTERN = re.compile(r"https?://(?:www\.)?imdb\.com/title/(tt\d+)")
IMDB_ID_PATTERN = re.compile(r"(tt\d+)")


def build_hde_page_url(page_number: int) -> str:
    """根据页码构造 HDE 电影列表页 URL。"""
    return f"{HDE_URL}tag/movies/page/{page_number}/"


def should_stop_scrapy(result_list: List[Dict[str, str]], end_title: str) -> bool:
    """当前批次命中截止标题时返回 True。"""
    return any(result_item.get("title") == end_title for result_item in result_list)


def scrapy_hde(start_page: int = 1, end_title: str = DEFAULT_END_TITLE) -> None:
    """
    从 ``start_page`` 开始连续抓取，直到命中 ``end_title`` 为止。

    每一页会先解析列表，再并发访问详情页写出 ``.rls`` 文件。
    这里默认使用配置文件中的截止标题和线程数，调用时也可以显式覆盖。
    """
    logger.info("抓取 hde 站点发布信息")
    while True:
        logger.info(f"抓取第 {start_page} 页")
        url = build_hde_page_url(start_page)
        response = get_hde_response(url)
        result_list = parse_hde_response(response)
        logger.info(f"共 {len(result_list)} 个结果")

        process_all(result_list, max_workers=DEFAULT_MAX_WORKERS)

        # 检查日期
        if should_stop_scrapy(result_list, end_title):
            logger.info("没有新发布，完成")
            break

        logger.warning("-" * 255)
        start_page += 1


def process_all(result_list, max_workers=5):
    """并发处理一批详情页任务，单个任务失败只记录日志，不中断整批。"""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {
            executor.submit(visit_hde_url, item): item
            for item in result_list
        }
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                future.result()
            except Exception as exc:
                logger.error(f"[ERROR] {item} -> {exc!r}")


@retry(
    stop_max_attempt_number=RETRY_MAX_ATTEMPTS,
    wait_random_min=RETRY_WAIT_MIN_MS,
    wait_random_max=RETRY_WAIT_MAX_MS,
)
def get_hde_response(url: str) -> requests.Response:
    """请求页面并统一做编码设置与状态码校验。"""
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
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
    """从标题末尾提取体积信息，失败时回退到配置中的默认体积。"""
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
    response = get_hde_response(url)
    soup = BeautifulSoup(response.text, 'lxml')
    result_item["imdb"] = extract_imdb_id_from_soup(soup)
    path = os.path.join(OUTPUT_DIR, build_hde_output_filename(result_item))
    write_list_to_file(path, [url])


def extract_imdb_id_from_soup(soup: BeautifulSoup) -> str:
    """从详情页所有链接中提取 IMDb 编号。"""
    return extract_imdb_id_from_links(a["href"] for a in soup.find_all("a", href=True))


def extract_imdb_id_from_links(hrefs: Iterable[str]) -> str:
    """
    优先从标准 IMDb 标题页 URL 提取，其次回退到宽松 ``tt`` 编号匹配。
    """
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


def build_hde_output_filename(result_item: Dict[str, str]) -> str:
    """根据发布信息拼装输出文件名，格式为 ``标题 - hde (体积)[IMDb].rls``。"""
    file_name = normalize_release_title_for_filename(result_item["title"])
    file_name = sanitize_filename(file_name)
    return f"{file_name} - hde ({result_item['size']})[{result_item.get('imdb', '')}].rls"
