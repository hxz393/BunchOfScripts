"""
抓取 onk 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import datetime
import logging
import os
import re

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from retrying import retry
from urllib3.util.retry import Retry

from my_module import read_json_to_dict, sanitize_filename, write_list_to_file

CONFIG_PATH = 'config/scrapy_onk.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

GROUP_DICT = CONFIG['group_dict']  # 栏目 ID 字典
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录
ONK_URL = CONFIG['onk_url']  # ONK 站点地址
ONK_COOKIE = CONFIG['onk_cookie']  # 用户甜甜
REQUEST_HEAD = CONFIG['request_head']  # 请求头
REQUEST_HEAD["Cookie"] = ONK_COOKIE  # 请求头加入认证

START_URL = ONK_URL + "/forums/{fid}/page-{page}?order=post_date&direction=desc"

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

retry_strategy = Retry(
    total=15,  # 总共重试次数
    status_forcelist=[502],  # 触发重试状态码
    method_whitelist=["POST", "GET"],  # 允许重试方法
    backoff_factor=1  # 重试等待间隔（指数增长）
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=8, pool_maxsize=16)
session = requests.Session()
session.proxies = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890", }
session.mount("http://", adapter)
session.mount("https://", adapter)


def scrapy_onk(start_page: int = 1, end_time: str = "2020-09-21") -> None:
    """
    抓取新发布内容写入到文件。
    """
    logger.info("抓取 onk 站点发布信息")
    stop_date = datetime.datetime.strptime(end_time, "%Y-%m-%d")
    for group_name, group_id in GROUP_DICT.items():
        # 一次一个栏目
        all_results = []
        start_page = 1
        while True:
            logger.info(f"爬取栏目 {group_name} ，爬取页面 {start_page} …")
            results, stop = parse_forum_page(group_id, start_page, stop_date)
            if not results:
                logger.info(f"栏目 {group_name} 没有爬取结果")
                break
            logger.info(f"共 {len(results)} 个帖子")

            # 将所有帖子写入到本地
            write_to_disk(results)
            logger.info("-" * 255)
            # 如果某一页有帖子早于停止日期，就停止翻页
            if stop:
                break
            start_page += 1
        logger.info("-" * 255)


def write_to_disk(result_list: list) -> None:
    """写入到磁盘"""
    # invalid_chars = r'[\\/:*?"<>|]'  # Windows 不允许的字符
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    for i in result_list:
        name = i['title']
        name = fix_name(name)
        name = sanitize_filename(name)
        file_name = f"{name}({i['label']})[{i['imdb_id']}].onk"
        path = os.path.join(OUTPUT_DIR, file_name)
        links = [i["url"], str(i["post_date"])]
        write_list_to_file(path, links)


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


def parse_forum_page(group_id, start_page, stop_time):
    """ 获取某个栏目的所有帖子 """
    url = START_URL.format(fid=group_id, page=start_page)
    resp = get_onk_response(url)
    soup = BeautifulSoup(resp.content, "lxml")

    # 每一条帖子
    items = soup.select('div.structItem.structItem--thread')
    if items is None:
        logger.error("没有找到帖子！样式更新了？")
        return [], False

    results = []
    stop = False

    for item in items:
        data = {}
        # 1. 标签（可选）
        label_span = item.select_one('a.labelLink span')
        data["label"] = label_span.get_text(strip=True) if label_span else ""

        # 2. 标题
        title_a = item.select_one('.structItem-title > a[href^="/threads/"]')
        if not title_a:
            continue  # 标题都没有，直接跳过

        # 3. 链接
        data["title"] = title_a.get_text(strip=True)
        data["url"] = ONK_URL + title_a["href"]

        # 4. IMDB 编号（可选）
        imdb_a = item.select_one('span.imdb a[href*="imdb.com/title"]')
        if imdb_a:
            match = re.search(r'(tt\d+)', imdb_a["href"])
            data["imdb_id"] = match.group(1) if match else None
        else:
            data["imdb_id"] = ""

        # 5. 发帖时间（取 structItem-startDate 下的 time.u-dt）
        time_tag = item.select_one('.structItem-startDate time.u-dt')
        post_date = None
        if time_tag and time_tag.has_attr("datetime"):
            # 2026-01-16T15:51:20+0000 → 2026-01-16
            # 尝试解析为 datetime 简单分割提取日期部分
            #
            date_str = time_tag["datetime"].split("T")[0]
            post_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
            data["post_date"] = post_date
        else:
            data["post_date"] = None

        # 判断停止时间
        if post_date:
            if post_date >= stop_time:
                results.append(data)
            elif start_page != 1:  # 判断是否达到停止条件
                stop = True
        else:
            results.append(data)

    return results, stop


@retry(stop_max_attempt_number=15, wait_random_min=15000, wait_random_max=20000)
def get_onk_response(url: str) -> requests.Response:
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
