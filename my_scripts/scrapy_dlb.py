"""
抓取 dlb 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from retrying import retry
from urllib3.util.retry import Retry

import requests
from bs4 import BeautifulSoup
from retrying import retry

from my_module import read_json_to_dict, sanitize_filename, write_list_to_file


CONFIG_PATH = 'config/scrapy_dlb.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件
DLB_COOKIE = CONFIG['dlb_cookie']  # 用户甜甜
REQUEST_HEAD = CONFIG['request_head']  # 请求头
REQUEST_HEAD["Cookie"] = DLB_COOKIE  # 请求头加入认证

DLB_URL = CONFIG['dlb_url']  # hde 地址
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录


logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

retry_strategy = Retry(
    total=15,  # 总共重试次数
    status_forcelist=[502],  # 触发重试状态码
    method_whitelist=["POST", "GET"],  # 允许重试方法
    backoff_factor=1  # 重试等待间隔（指数增长）
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=20, pool_maxsize=40)
session = requests.Session()
session.proxies = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890", }
session.mount("http://", adapter)
session.mount("https://", adapter)


def scrapy_dlb(start_page: int = 1, end_title="Tawai.A.voice.from.the.forest.2017.1080p.BluRay.REMUX.AVC.DTS-HD.MA.5.1-EPSiLON – 22.4 GB") -> None:
    """
    抓取发布信息写入到文件。
    """
    logger.info("抓取 dlb 站点发布信息")
    while True:
        logger.info(f"抓取第 {start_page} 页")
        url = f"{DLB_URL}/cat/movie/page/{start_page}/"
        response = get_dlb_response(url)
        result_list = parse_dlb_response(response)
        logger.info(f"共 {len(result_list)} 个结果")

        # 循环抓取
        # for list_item in result_list:
        #     visit_dlb_url(list_item)
        # return

        process_all(result_list, max_workers=20)

        # 检查日期
        if end_title in (result_item['title'] for result_item in result_list):
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
            executor.submit(visit_dlb_url, item): item
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


@retry(stop_max_attempt_number=15, wait_random_min=15000, wait_random_max=20000)
def get_dlb_response(url: str) -> requests.Response:
    """请求流程"""
    logger.info(f"访问 {url}")
    response = session.get(url, timeout=30, verify=False, headers=REQUEST_HEAD)
    # response.encoding = 'utf-8'
    if response.status_code != 200:
        logger.error(f"请求失败，重试 {response.status_code}：{url}")
        raise Exception(f"请求失败")

    if len(response.text) < 10000:
        logger.error(f"请求被封锁，重试：{url}\n{response.text}")
        raise Exception(f"请求被封锁")

    return response


def parse_dlb_response(response: requests.Response) -> list:
    """解析HTML"""
    soup = BeautifulSoup(response.text, "html.parser")

    results = []

    for block in soup.select("div.movies_block"):
        # 标题
        title_tag = block.select_one("span.movie_title_list_text")
        title = title_tag.get_text(strip=True) if title_tag else ""

        # 链接
        link_tag = block.select_one("div.movie_title_list a")
        href = link_tag["href"] if link_tag and "href" in link_tag.attrs else ""

        # 大小
        size_tag = block.select_one("div.type_banner_size")
        size = size_tag.get_text(strip=True) if size_tag else ""

        results.append({
            "title": title,
            "link": DLB_URL + href,
            "size": size.replace(' ', '')
        })

    return results


def visit_dlb_url(result_item: dict):
    """访问详情页"""
    url = result_item["link"]
    response = get_dlb_response(url)
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
    file_name = f"{file_name} ({result_item['size']})[{imdb_id}].dlb"
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
