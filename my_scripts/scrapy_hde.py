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
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
from retrying import retry

from my_module import read_json_to_dict, sanitize_filename, write_list_to_file

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG_PATH = 'config/scrapy_hde.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

HDE_URL = CONFIG['hde_url']  # hde 地址
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录


def scrapy_hde(start_page: int = 1, end_title="Tawai.A.voice.from.the.forest.2017.1080p.BluRay.REMUX.AVC.DTS-HD.MA.5.1-EPSiLON – 22.4 GB") -> None:
    """
    抓取发布信息写入到文件。
    """
    logger.info("抓取 hde 站点发布信息")
    while True:
        logger.info(f"抓取第 {start_page} 页")
        url = f"{HDE_URL}tag/movies/page/{start_page}/"
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
        if end_title in (result_item['title'] for result_item in full_list):
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
    soup = BeautifulSoup(response.text, "lxml")
    results = []
    for fit in soup.select("div.fit.item"):
        data_div = fit.select_one("div.data")
        if not data_div:
            continue
        a_tag = data_div.select_one("h5 a")
        if not a_tag:
            # 有时候 h5 里可能直接是文字或结构不同，尝试取 h5 的第一个链接或文字
            a_tag = data_div.select_one("h5 > a, h5 a")
        if not a_tag:
            continue
        title = a_tag.get_text(strip=True)
        url = a_tag.get("href", "").strip()
        results.append({"title": title, "url": url})
    return results


def split_size(items: List[Dict[str, str]], default_size: str = "100.0 GB") -> List[Dict[str, str]]:
    """
    对 items 做后处理。items 中每个 dict 有 "title" 和 "url"。
    如果 title 中包含 “–” 分隔（或其他类似分隔符），提取 size，否则使用默认 size。
    返回新的 list，每个 dict 增加 "size" 字段。
    """
    normalized = []
    for it in items:
        title = it.get("title", "")
        size = default_size

        # 尝试用 “–” （长破折号）来分割
        # 注意：可能有其它破折号类型（如普通的 "-","—" 等），可以做多个检查
        # 下面用正则把最后的 “– XX GB” 提取出来
        # 匹配 “ – ” 后面的小数 + 单位
        m = re.search(r"\s[–-]\s*([\d.]+\s*(?:GB|MB|TB))\s*$", title)
        if m:
            size = m.group(1)
        else:
            # 如果没找到，用 fallback 方法：尝试更宽松的匹配
            m2 = re.search(r"([\d.]+\s*(?:GB|MB|TB))\s*$", title)
            if m2:
                size = m2.group(1)

        # 构造新的 dict（也可以修改原来的）
        new_it = {
            "title": title,
            "url": it.get("url", ""),
            "size": size.replace(" ", "")
        }
        normalized.append(new_it)
    return normalized


def visit_hde_url(result_item: dict):
    """访问详情页"""
    url = result_item["url"]
    logger.info(f"访问 {url}")
    response = get_hde_response(url)
    soup = BeautifulSoup(response.text, 'lxml')

    imdb_id = ""

    # 找所有 a 标签，href 包含 /title/tt
    for a in soup.find_all('a', href=True):
        href = a['href']
        # 查找 imdb title 链接
        m = re.search(r"https?://(?:www\.)?imdb\.com/title/(tt\d+)", href)
        if m:
            imdb_id = m.group(1)
            break

    # 如果找不到，可以尝试更宽松匹配
    if imdb_id is None:
        for a in soup.find_all('a', href=True):
            href = a['href']
            m2 = re.search(r"(tt\d+)", href)
            if m2:
                imdb_id = m2.group(1)
                break

    # 存回 result_item 或返回
    result_item["imdb"] = imdb_id
    file_name = fix_name(result_item['title'])
    file_name = sanitize_filename(file_name)
    file_name = f"{file_name} - hde ({result_item['size']})[{imdb_id}].rls"
    path = os.path.join(OUTPUT_DIR, file_name)
    write_list_to_file(path, [url])


def fix_name(name: str, max_length: int = 220) -> str:
    """修剪文件名"""
    name = re.sub(r'\s*\|\s*', '，', name)
    name = re.sub(r'\s*/\s*', '｜', name)
    name = re.sub(r'\s*\\s*', '｜', name)
    name = re.sub(r'\s+', ' ', name)
    name = name.replace("\t", " ").strip()
    name = name.replace("{@}", ".").strip()
    # 长度不超限，直接返回
    if len(name) <= max_length:
        return name
    else:
        return name[:max_length]
