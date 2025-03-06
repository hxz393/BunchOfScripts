"""
从服务器 qBittorrent 获取下载中的磁链，保存到本地

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
from pathlib import Path
from typing import Optional

import requests

from add_to_qb import qb_login
from my_module import read_json_to_dict

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG = read_json_to_dict('config/add_to_qb.json')  # 配置文件

QB_URL = CONFIG['qb_url']  # qb 地址
QB_SAVE_DIR = CONFIG['qb_save_dir']  # qb 保存目录
MAGNET_PATH = CONFIG['magnet_path']  # 输出目录


def get_qb_downloads(target: str) -> None:
    """
    从 QB 获取下载任务信息到 JSON，转存磁链到本地 log 文件

    :param target: 目标目录
    :return: 无
    """
    # 先登录 QB
    session = requests.Session()
    if not qb_login(session):
        return

    # 获取总体信息
    torrents = get_qb_torrents(session)
    if not torrents:
        return

    for torrent in torrents:
        magnet_uri = torrent['magnet_uri']
        file_name = f"{os.path.basename(torrent['save_path'])}.log"
        file_path = os.path.join(target, torrent['tags'], file_name)
        target_path = Path(file_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with target_path.open('w', encoding="utf-8") as file:
            file.write(magnet_uri)


def get_qb_torrents(session: requests.Session) -> Optional[dict]:
    """
    请求 QB 获取 JSON 响应会返回

    :param session: 会话
    :return: json 数据
    """
    torrents_url = f"{QB_URL}/api/v2/torrents/info?filter=downloading"
    # 获取正在下载的任务信息
    torrents_response = session.get(torrents_url)
    # 登录成功时，返回内容通常为 "Ok."
    if torrents_response.status_code != 200:
        logger.error(f"请求 QB 失败：{torrents_response.status_code} {torrents_response.text}")
        return
    return torrents_response.json()
