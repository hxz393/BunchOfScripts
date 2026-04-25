"""
抓取 sk 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from retrying import retry

from my_module import normalize_release_title_for_filename, read_json_to_dict, sanitize_filename, write_list_to_file
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

REQUEST_HEAD["Cookie"] = SK_COOKIE  # 请求头加入认证


def scrapy_sk(start_page: int = 0, end_data="15/10/2013") -> None:
    """
    抓取发布信息写入到文件。
    """
    logger.info("抓取 sk 站点发布信息")
    while True:
        if process_sk_page(start_page, end_data):
            logger.info("没有新发布，完成")
            break

        logger.warning("-" * 255)
        start_page += 1


def process_sk_page(page_no: int, end_data: str) -> bool:
    """抓取并处理单个 SK 列表页。"""
    logger.info(f"抓取第 {page_no} 页")
    url = f"{SK_MOVIE_URL}{page_no}"
    response = get_sk_response(url)
    result_list = parse_sk_response(response)
    logger.info(f"共 {len(result_list)} 个结果")
    process_all(result_list)
    return end_data in (result_item['date'] for result_item in result_list)


def process_all(result_list):
    """
    并发调用 visit_sk_url，result_list 中每个元素都会被提交到线程池执行。
    """
    results = []
    with ThreadPoolExecutor(THREAD_NUMBER) as executor:
        # 提交所有任务
        future_to_item = {
            executor.submit(visit_sk_url, item): item
            for item in result_list
        }
        # 按完成顺序收集结果或捕获异常
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                ret = future.result()
            except Exception as exc:
                logger.error(f"[ERROR] {item} -> {exc!r}")
            else:
                results.append(ret)
    return results


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

    if not (group and url and title):
        return None

    return {
        "group": group,
        "url": url,
        "title": title,
    }


def parse_sk_row(td) -> dict | None:
    """解析单个 SK 列表项。"""
    links = extract_sk_row_links(td)
    if not links:
        logger.info("跳过：缺少链接字段")
        return None

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
        return

    csfd_data = get_normalized_csfd_data(csfd_url)
    file_name = build_sk_output_filename(result_item, csfd_data)
    path = os.path.join(OUTPUT_DIR, file_name)
    write_list_to_file(path, [url])
