"""
抓取 sk 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import re

import requests
from bs4 import BeautifulSoup
from retrying import retry
from concurrent.futures import ThreadPoolExecutor, as_completed

from my_module import read_json_to_dict, sanitize_filename, write_list_to_file
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

REQUEST_HEAD["Cookie"] = SK_COOKIE  # 请求头加入认证


def scrapy_sk(start_page: int = 0, end_data="15/10/2013") -> None:
    """
    抓取发布信息写入到文件。
    """
    logger.info("抓取 sk 站点发布信息")
    while True:
        # 请求 sk 主页
        logger.info(f"抓取第 {start_page} 页")
        url = f"{SK_MOVIE_URL}{start_page}"
        response = get_sk_response(url)
        result_list = parse_sk_response(response)
        logger.info(f"共 {len(result_list)} 个结果")

        # 循环抓取
        # for result_item in result_list:
        #     visit_sk_url(result_item)
        process_all(result_list, max_workers=25)

        # 检查日期
        if end_data in (result_item['date'] for result_item in result_list):
            logger.info("没有新发布，完成")
            break

        # logger.info(f"结果：{result_list}")
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
    response = requests.get(url, headers=REQUEST_HEAD)
    response.encoding = 'utf-8'
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    return response


def parse_sk_response(response: requests.Response) -> list:
    """解析流程"""
    soup = BeautifulSoup(response.text, "html.parser")

    # 找到每一行
    rows = soup.select("table.lista table.lista td.lista")

    # rows 就是一个 list，里面每个元素都是一个 <td class="lista"> Tag
    results = []
    for td in rows:
        group = None
        url = None
        title = None

        # 1) 遍历所有 <a>，按 href 判断是“组别”还是“详情”
        for a in td.find_all('a', href=True):
            h = a['href']
            if h.startswith("torrents_v2.php?category"):
                group = a.get_text(strip=True)
            elif h.startswith("details.php?name"):
                url = SK_URL + "torrent/" + h
                title = a.get_text(strip=True)

        # 如果任一关键字段没找到就跳过
        if not (group and url and title):
            logger.info(f"跳过：{title} - {url}")
            continue

        # 2) 抓出“Velkost ... | Pridany ...”这段纯文本
        size = date = None
        for s in td.stripped_strings:
            if s.startswith("Velkost"):
                # "Velkost 2.4 GB | Pridany 27/07/2025"
                part_size, part_date = [p.strip() for p in s.split("|", 1)]
                size = part_size.replace("Velkost ", "")
                date = part_date.replace("Pridany ", "")
                break

        if not (size and date):
            logger.info(f"跳过：{title} - {url}")
            continue

        results.append({
            "group": group,
            "url": url,
            "title": title,
            "size": size,
            "date": date
        })

    return results


def fix_name(name: str, max_length: int = 220) -> str:
    """修剪文件名"""
    name = re.sub(r'\s*\|\s*', '，', name)
    name = re.sub(r'\s*/\s*', '｜', name)
    name = re.sub(r'\s*\\s*', '｜', name)
    name = re.sub(r'\s+', ' ', name)
    name = re.sub(r'\s*=\s*CSFD\s*\d+%', '', name)
    name = name.replace("\t", " ").strip()
    name = name.replace("{@}", ".").strip()
    # 长度不超限，直接返回
    if len(name) <= max_length:
        return name
    else:
        return name[:max_length]


def visit_sk_url(result_item: dict):
    """访问详情页"""
    url = result_item["url"]
    logger.info(f"访问 {url}")
    response = get_sk_response(url)
    soup = BeautifulSoup(response.text, 'lxml')

    # 选出 <img>，然后取它的父节点 <a>，获取 CSFD 链接
    img = soup.select_one('a[itemprop="sameAs"] > img[src="/torrent/images/csfd.png"]')
    csfd_url = None
    if img:
        csfd_url = img.parent['href']

    if not csfd_url:
        return

    response = get_csfd_response(csfd_url)
    csfd_data = get_csfd_movie_details(response)

    if not csfd_data["id"]:
        csfd_data["id"] = "csfd" + csfd_url.split("/")[-1]
    file_name = result_item['title'] + "#" + csfd_data['origin'] + "#" + "{" + csfd_data['director'] + "}"
    file_name = fix_name(file_name)
    file_name = sanitize_filename(file_name) + "(" + result_item['size'] + ")" + "[" + csfd_data["id"] + "]"
    file_name = f"{file_name}.sk"
    print(file_name)
    path = os.path.join(OUTPUT_DIR, file_name)
    write_list_to_file(path, [url])
