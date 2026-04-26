"""
抓取 hde 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
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
requests.packages.urllib3.disable_warnings()

CONFIG_PATH = 'config/scrapy_hde.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

HDE_URL = CONFIG['hde_url']  # hde 地址
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录

SIZE_WITH_DASH_PATTERN = re.compile(r"\s[–-]\s*([\d.]+\s*(?:GB|MB|TB))\s*$")
TRAILING_SIZE_PATTERN = re.compile(r"([\d.]+\s*(?:GB|MB|TB))\s*$")
IMDB_URL_PATTERN = re.compile(r"https?://(?:www\.)?imdb\.com/title/(tt\d+)")
IMDB_ID_PATTERN = re.compile(r"(tt\d+)")


def build_hde_page_url(page_number: int) -> str:
    """构造列表页 URL。"""
    return f"{HDE_URL}tag/movies/page/{page_number}/"


def should_stop_scrapy(result_list: List[Dict[str, str]], end_title: str) -> bool:
    """当前批次命中截止标题时返回 True。"""
    return any(result_item.get("title") == end_title for result_item in result_list)


def scrapy_hde(start_page: int = 1, end_title="Tawai.A.voice.from.the.forest.2017.1080p.BluRay.REMUX.AVC.DTS-HD.MA.5.1-EPSiLON – 22.4 GB") -> None:
    """
    抓取发布信息写入到文件。
    """
    logger.info("抓取 hde 站点发布信息")
    while True:
        logger.info(f"抓取第 {start_page} 页")
        url = build_hde_page_url(start_page)
        response = get_hde_response(url)
        result_list = parse_hde_response(response)
        logger.info(f"共 {len(result_list)} 个结果")

        full_list = split_size(result_list)

        # 循环抓取
        # for list_item in full_list:
        #     visit_hde_url(list_item)
        # return

        process_all(full_list, max_workers=30)

        # 检查日期
        if should_stop_scrapy(full_list, end_title):
            logger.info("没有新发布，完成")
            break

        # logger.info(f"结果：{result_list}")
        logger.warning("-" * 255)
        start_page += 1


def process_all(result_list, max_workers=5):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_item = {
            executor.submit(visit_hde_url, item): item
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


@retry(stop_max_attempt_number=150, wait_random_min=1000, wait_random_max=10000)
def get_hde_response(url: str) -> requests.Response:
    """请求流程"""
    response = requests.get(url, timeout=30)
    response.encoding = 'utf-8'
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    return response


def parse_hde_response(response: requests.Response) -> list:
    """解析 HDE 单页列表，输出结果列表"""
    soup = BeautifulSoup(response.text, "lxml")
    results = []
    for fit in soup.select("div.fit.item"):
        result_item = parse_hde_item(fit)
        if result_item:
            results.append(result_item)
    return results


def parse_hde_item(fit) -> Dict[str, str] | None:
    """解析单个列表条目。"""
    data_div = fit.select_one("div.data")
    if not data_div:
        return None

    a_tag = data_div.select_one("h5 a")
    if not a_tag:
        # 有时候 h5 里可能直接是文字或结构不同，尝试取 h5 的第一个链接
        a_tag = data_div.select_one("h5 > a, h5 a")
    if not a_tag:
        return None

    title = a_tag.get_text(strip=True)
    url = a_tag.get("href", "").strip()
    return {"title": title, "url": url}


def split_size(items: List[Dict[str, str]], default_size: str = "100.0 GB") -> List[Dict[str, str]]:
    """
    对 items 做后处理。items 中每个 dict 有 "title" 和 "url"。
    如果 title 中包含 “–” 分隔（或其他类似分隔符），提取 size，否则使用默认 size。
    返回新的 list，每个 dict 增加 "size" 字段。
    """
    normalized = []
    for it in items:
        title = it.get("title", "")
        size = extract_release_size(title, default_size=default_size)

        # 构造新的 dict（也可以修改原来的）
        new_it = {
            "title": title,
            "url": it.get("url", ""),
            "size": size
        }
        normalized.append(new_it)
    return normalized


def extract_release_size(title: str, default_size: str = "100.0 GB") -> str:
    """从标题末尾提取体积信息。"""
    match = SIZE_WITH_DASH_PATTERN.search(title)
    if not match:
        match = TRAILING_SIZE_PATTERN.search(title)
    if match:
        return match.group(1).replace(" ", "")
    return default_size.replace(" ", "")


def visit_hde_url(result_item: dict):
    """访问详情页"""
    url = result_item["url"]
    logger.info(f"访问 {url}")
    response = get_hde_response(url)
    soup = BeautifulSoup(response.text, 'lxml')
    result_item["imdb"] = extract_imdb_id_from_soup(soup)
    path = os.path.join(OUTPUT_DIR, build_hde_output_filename(result_item))
    write_list_to_file(path, [url])


def extract_imdb_id_from_soup(soup: BeautifulSoup) -> str:
    """从详情页中提取 IMDb 编号。"""
    hrefs = [a["href"] for a in soup.find_all("a", href=True)]
    return extract_imdb_id_from_links(hrefs)


def extract_imdb_id_from_links(hrefs: Iterable[str]) -> str:
    """优先从标准 IMDb URL 提取，其次宽松匹配 ``tt`` 编号。"""
    href_list = list(hrefs)
    for href in href_list:
        match = IMDB_URL_PATTERN.search(href)
        if match:
            return match.group(1)

    for href in href_list:
        match = IMDB_ID_PATTERN.search(href)
        if match:
            return match.group(1)

    return ""


def build_hde_output_filename(result_item: Dict[str, str]) -> str:
    """根据发布信息拼装输出文件名。"""
    file_name = normalize_release_title_for_filename(result_item["title"])
    file_name = sanitize_filename(file_name)
    return f"{file_name} - hde ({result_item['size']})[{result_item.get('imdb', '')}].rls"
