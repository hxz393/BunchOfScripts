"""
抓取 rlsbb 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from retrying import retry

from my_module import read_json_to_dict, sanitize_filename, write_list_to_file

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG_PATH = 'config/scrapy_rls.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

RLS_URL = CONFIG['rls_url']  # rlsbb 地址
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录


def scrapy_rls(start_page: int = 1, f_mode=True, end_title="The Gangster The Cop The Devil 2019 HDRip AC3 X264-CMRG (1.35GB)") -> None:
    """
    抓取发布信息写入到文件。
    """
    logger.info("抓取 rlsbb 站点发布信息")
    while True:
        logger.info(f"抓取第 {start_page} 页")
        url = f"{RLS_URL}category/foreign-movies/page/{start_page}/?s=" if f_mode else f"{RLS_URL}category/movies/page/{start_page}/?s="
        while True:
            response = get_rls_response(url)
            result_list = parse_rls_response(response)
            if len(result_list):
                break
            time.sleep(3)
            logger.warning("等待 3 秒后重试")
        logger.info(f"共 {len(result_list)} 个结果")

        # # 循环抓取
        # for list_item in result_list:
        #     visit_rls_url(list_item)
        # return

        process_all(result_list, max_workers=40)

        # 终止检查
        if end_title.replace(" ", ".") in (result_item['title'] for result_item in result_list):
            logger.info("没有新发布，完成")
            break

        logger.warning("-" * 255)
        start_page += 1


def process_all(result_list, max_workers=5):
    """
    并发调用 visit_sk_url，result_list 中每个元素都会被提交到线程池执行。
    max_workers 控制并发线程数，视网络 I/O 或目标服务器承受能力调整。
    """
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_item = {
            executor.submit(visit_rls_url, item): item
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
def get_rls_response(url: str) -> requests.Response:
    """请求流程"""
    response = requests.get(url, timeout=35)
    response.encoding = 'utf-8'
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    return response


def parse_rls_response(response: requests.Response) -> list:
    """解析响应文本"""
    soup = BeautifulSoup(response.text, "html.parser")
    results = []
    # 找到所有 class="p-c p-c‑title" 的 div
    for title_div in soup.find_all("div", class_="p-c p-c-title"):
        a_tag = title_div.find("h2").find("a")
        title_text = a_tag.get_text(strip=True)
        url = a_tag.get("href")
        # 替换无关符号
        title_text = title_text.replace(" ⭐", "").replace(" ", ".")

        # # 用正则提取括号里面的大小 (比如 "394MB" 或 "1.45GB")
        # m = re.search(r"\(([\d.]+\s*(?:GB|MB|TB))\)", title_text)
        # if m:
        #     size = m.group(1)
        # else:
        #     size = "100GB"  # 如果没匹配到，就设默认值

        results.append({
            "title": title_text,
            "url": url
        })

    return results


def visit_rls_url(result_item: dict):
    """访问详情页"""
    url = result_item["url"]
    logger.info(f"访问 {url}")
    response = get_rls_response(url)
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
    file_name = f"{file_name} - rls [{imdb_id}].rls"
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
