"""
批量添加种子到 115 离线

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os

import requests
from p115client import P115Client
from retrying import retry

from my_module import read_json_to_dict, read_file_to_list

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG = read_json_to_dict('config/add_to_115.json')  # 配置文件

COOKIE_115 = CONFIG['cookie_115']  # 帐号 Cookie
MAGNET_PATH = CONFIG['magnet_path']  # 磁链前缀


@retry(stop_max_attempt_number=3, wait_random_min=100, wait_random_max=1200)
def add_to_115(source: str) -> None:
    """
    获取 JSON 或 LOG 文件中的下载链接，添加到 115 离线

    :param source: 来源目录
    :return: 无
    """
    client = P115Client(COOKIE_115)
    # 遍历文件夹
    for root, dirs, files in os.walk(source):
        for file_name in files:
            # 取当前文件夹的名称作为 director
            director = os.path.basename(root)
            # 拼接出完整路径
            file_path = os.path.join(root, file_name)
            # 去掉扩展名，得到文件名字段
            file_name_no_ext = os.path.splitext(file_name)[0]
            if len(file_name_no_ext) > 100:
                file_name_no_ext = file_name_no_ext[:100] + '...'

            # 判断是否为 .json 文件
            if file_name.endswith('.json'):
                # 读取 json 文件，获取下载链接
                dl_info = {t['size_bytes']: t['hash'] for t in read_json_to_dict(file_path)['data']['movie']['torrents']}
                dl_link = f"{MAGNET_PATH}{dl_info[max(dl_info.keys())]}"
            elif file_name.endswith('.log'):
                # 读取 log 文件，获取下载链接
                dl_link = read_file_to_list(file_path)[0]
                dl_link = dl_link.replace('\ufeff', '')
            else:
                continue

            # 添加 115 离线
            r = client.offline_add_url(
                {
                    "url": dl_link,
                    "savepath": f"{director}/{file_name_no_ext}",
                }
            )
            print(r)
