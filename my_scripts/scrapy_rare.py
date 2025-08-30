"""
抓取 rare 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import re

import requests
from bs4 import BeautifulSoup, NavigableString
from retrying import retry
from concurrent.futures import ThreadPoolExecutor, as_completed

from my_module import read_json_to_dict, sanitize_filename, write_list_to_file
from sort_movie_request import get_csfd_response, get_csfd_movie_details
import concurrent.futures
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict

import requests
from lxml import etree
from retrying import retry

from my_module import read_json_to_dict, sanitize_filename, write_dict_to_json, read_file_to_list

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG_PATH = 'config/scrapy_rare.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

REQUEST_HEAD = CONFIG['request_head']  # 请求头
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录


def scrapy_rare(source_file: str) -> None:
    """
    抓取新发布内容写入到文件。
    """
    logger.info("抓取 rare 站点发布信息")

    # 获取url列表，多线程爬取
    links = read_file_to_list(source_file)
    process_all(links, max_workers=30)


def process_all(links, max_workers=5):
    """多线程访问链接"""
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_item = {
            executor.submit(visit_rare_url, link): link
            for link in links
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
def get_rare_response(url: str) -> requests.Response:
    """请求流程"""
    response = requests.get(url, headers=REQUEST_HEAD)
    response.encoding = 'utf-8'
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    return response


def parse_response(response: requests.Response) -> dict:
    """解析流程"""
    result_dict = {"file_name": "", "content": ""}
    # print(response.text)
    soup = BeautifulSoup(response.text, "html.parser")
    # 主要内容
    entry = soup.select_one("div.entry-content")
    if not entry:
        entry = soup.select_one("div.entry")
    if not entry:
        return {}

    lines = []
    for p in entry.select("p"):
        # 将 <a> 替换为 “文本 (链接)”；若无文本（如包着图片），就仅保留链接
        for a in p.find_all("a", href=True):
            text = a.get_text(strip=True)
            href = a["href"]
            replacement = f"{text} ({href})" if text else href
            a.replace_with(NavigableString(replacement))

        # 去掉图片标签
        for img in p.find_all("img"):
            img.decompose()

        # 把 <br> 当作空格，整段收敛为一行
        line = p.get_text(separator=" ", strip=True)
        if line:
            lines.append(line)
    # 处理 <pre> 标签
    for pre in entry.find_all("pre"):
        pre_text = pre.get_text(separator=" ", strip=True)
        if pre_text:
            lines.append(pre_text)
    # 处理 <figure> 标签中的 <a> 链接
    for fig_a in entry.select("figure a[href]"):
        href = fig_a["href"]
        lines.append(f"{href}")  # 单独添加 URL
    # 处理 <h4> 标签中的 <a> 链接
    for h4_a in entry.select("h4 a[href]"):
        href = h4_a["href"]
        lines.append(f"{href}")  # 单独添加 URL
    content = "\n".join(lines)
    result_dict['content'] = content

    # 标题
    title_tag = soup.title
    title = title_tag.string if title_tag else ""
    # imdb 编号
    m = re.search(r'/title/(tt\d+)', content)
    imdb = m.group(1) if m else ""
    # 拼凑文件名
    file_name = sanitize_filename(title) + "[" + imdb + "]"
    file_name = f"{file_name}.rare"
    result_dict['file_name'] = file_name

    return result_dict


def visit_rare_url(link: str):
    """访问详情页"""
    url = link
    logger.info(f"访问 {url}")
    response = get_rare_response(url)
    result_dict = parse_response(response)

    path = os.path.join(OUTPUT_DIR, result_dict['file_name'])
    write_list_to_file(path, [url, result_dict['content']])
