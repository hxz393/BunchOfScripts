"""
抓取 mt 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import re

import requests
from requests.adapters import HTTPAdapter
from retrying import retry
from urllib3.util.retry import Retry

from my_module import read_json_to_dict, sanitize_filename, write_list_to_file, format_size, write_dict_to_json

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG_PATH = 'config/scrapy_mt.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

MT_API_URL = CONFIG['mt_api_url']  # mt 地址
MT_AUTH = CONFIG['mt_auth']  # 用户认证

MT_SIGN = CONFIG['mt_sign']  # 请求验证加签，真实请求一次后获取
MT_TIME = CONFIG['mt_time']  # 请求验证加签对应时间戳

REQUEST_HEAD = CONFIG['request_head']  # 请求头
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录

REQUEST_HEAD["Authorization"] = MT_AUTH  # 请求头加入认证

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


def scrapy_mt(start_time="2011-01-01", end_time="2015-01-01") -> None:
    """
    抓取发布信息写入到文件。
    """
    logger.info("先获取区间页数")
    r_first = post_mt_response(start_time, end_time, 100)
    result_first = r_first.json()
    if result_first['code'] != '0' or result_first['message'] != 'SUCCESS':
        raise Exception(f"获取区间页数失败 {result_first}")

    data_first = result_first['data']
    total_page = data_first['totalPages']
    if total_page == '100':
        raise Exception(f"时间范围过大，页数超过最大限制 {result_first}")
    logger.info(f"获取区间页数成功，{start_time} - {end_time} 共 {total_page} 页")

    # 循环抓取页面
    start_page = 0
    while start_page < int(total_page):
        start_page += 1
        logger.info(f'访问第 {start_page} 页')
        r = post_mt_response(start_time, end_time, start_page)
        result = r.json()
        items_list = result['data']['data']
        parse_mt_response(items_list)
        # print(items_list)
        # return


def parse_mt_response(items_list):
    """解析json并写入文件"""
    for item_dict in items_list:
        name = item_dict['name']
        desc = item_dict['smallDescr']
        imdb = m.group(1) if (m := re.search(r'(tt\d+)', item_dict['imdb'])) else ""
        size = format_size(int(item_dict['size'])).replace(" ", "")
        main_name = fix_name(f"{name}[{desc}]")
        main_name = sanitize_filename(main_name)

        # 拼凑文件名
        file_name = f"{main_name}({size})[{imdb}].ptmt"
        path = os.path.join(OUTPUT_DIR, file_name)
        write_dict_to_json(path, item_dict)


def fix_name(name: str, max_length: int = 220) -> str:
    """修剪文件名"""
    name = re.sub(r'\s*/\s*', '｜', name)
    name = re.sub(r'\s*\\s*', '｜', name)
    name = re.sub(r'\s+', ' ', name)
    name = name.replace("\t", " ").strip()
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


@retry(stop_max_attempt_number=15, wait_random_min=1000, wait_random_max=10000)
def post_mt_response(s_date: str, e_date: str, page: int) -> requests.Response:
    """请求流程"""
    data = {
        "categories": [],
        "mode": "movie",
        "pageNumber": int(page),
        "pageSize": 100,
        "uploadDateStart": f"{s_date} 00:00:00",
        "uploadDateEnd": f"{e_date} 00:00:00",
        "visible": 0,
        "_sgin": MT_SIGN,
        "_timestamp": MT_TIME
    }
    response = session.post(MT_API_URL, headers=REQUEST_HEAD, json=data)
    response.encoding = 'utf-8'
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：页面 {page}")

    return response
