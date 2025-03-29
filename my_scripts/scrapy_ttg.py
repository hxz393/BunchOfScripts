"""
抓取 ttg 站点发布信息

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

from my_module import read_json_to_dict, sanitize_filename, write_list_to_file, update_json_config

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG_PATH = 'config/scrapy_ttg.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

TTG_URL = CONFIG['ttg_url']  # ttg 地址
TTG_MOVIE_URL = CONFIG['ttg_movie_url']  # ttg 电影列表地址
NEWEST_ID = CONFIG['newest_id']  # 最新 id
TTG_COOKIE = CONFIG['ttg_cookie']  # 用户甜甜
REQUEST_HEAD = CONFIG['request_head']  # 请求头
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录

REQUEST_HEAD["Cookie"] = TTG_COOKIE  # 请求头加入认证


def scrapy_ttg() -> None:
    """
    抓取发布信息写入到文件。
    """
    logger.info("抓取 ttg 站点发布信息")
    max_ids = []
    page = 0
    while True:
        # 请求 TTG
        logger.info(f"抓取第 {page} 页")
        url = f"{TTG_MOVIE_URL}&&page={page}&"
        response = get_ttg_response(url)
        result_list = parse_ttg_response(response)

        # 比较最新 id，过滤结果
        new_list = filter_by_id(result_list)
        # 如果全过滤掉了，则不用继续了
        if len(new_list) == 0:
            logger.info("没有新发布")
            break

        # 写入到本地
        page += 1
        max_ids.append(max(int(movie['id']) for movie in new_list))
        write_to_disk(new_list)
    max_id = max(max_ids) if max_ids else NEWEST_ID
    update_json_config(CONFIG_PATH, "newest_id", max_id)


def filter_by_id(movie_list: list) -> list:
    """
    过滤 movie_list 中所有 id 小于 threshold_id 的字典元素
    """
    filtered_list = [movie for movie in movie_list if int(movie['id']) > NEWEST_ID]
    return filtered_list


@retry(stop_max_attempt_number=15, wait_random_min=1000, wait_random_max=10000)
def get_ttg_response(url: str) -> requests.Response:
    """请求流程"""
    response = requests.get(url, headers=REQUEST_HEAD)
    response.encoding = 'utf-8'
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    return response


def parse_ttg_response(response: requests.Response) -> list:
    """解析流程"""
    soup = BeautifulSoup(response.text, "html.parser")

    # 定位 id 为 torrent_table 的 table
    table = soup.find("table", id="torrent_table")
    # 找到所有 class 包含 hover_hr 的 tr 标签
    rows = table.find_all("tr", class_=lambda x: x and "hover_hr" in x)

    data_list = []
    for row in rows:
        # 提取 torrent id，即 tr 的 id 属性
        torrent_id = row.get("id")
        torrent_url = f"{TTG_URL}/t/{torrent_id}/"

        # 种子名称：在 a.treport 下的 img.report 中的 torrentname 属性
        torrent_img = row.select_one("a.treport img.report")
        torrent_name = torrent_img.get("torrentname") if torrent_img else ""

        # 下载地址：class 为 dl_a 的 a 标签的 href 属性
        dl_a = row.select_one("a.dl_a")
        download_href = dl_a.get("href") if dl_a else None
        download_url = f"{TTG_URL}{download_href}" if download_href else ""

        # imdb 地址：在 span.imdb_rate 下 a 标签的 href 属性
        imdb_a = row.select_one("span.imdb_rate a")
        imdb_url = imdb_a.get("href") if imdb_a else ""
        imdb_id = m.group(1) if (m := re.search(r'(tt\d+)', imdb_url)) else ""

        # 大小：该信息位于大小那一列，通常是第7个 td (索引6)
        tds = row.find_all("td")
        size = "".join(tds[6].stripped_strings) if len(tds) > 6 else ""

        data = {
            "id": torrent_id,
            "url": torrent_url,
            "name": torrent_name,
            "dl": download_url,
            "imdb": imdb_id,
            "size": size
        }
        data_list.append(data)

    return data_list


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


def write_to_disk(result_list: list) -> None:
    """写入到磁盘"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    for i in result_list:
        name = i['name']
        name = fix_name(name)
        name = sanitize_filename(name)
        file_name = f"{name}({i['size']})[{i['imdb']}].ttg"
        path = os.path.join(OUTPUT_DIR, file_name)
        links = [i["url"], i["dl"]]
        write_list_to_file(path, links)
