"""
批量添加种子到 pikpak 离线。使用第三方客户端：https://github.com/52funny/pikpakcli
配额查询：https://mypikpak.com/drive/all?action=show_traffic_dialog

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import subprocess

from my_module import read_json_to_dict, read_file_to_list
from sort_movie_ops import select_yts_best_torrent

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict('config/add_to_pikpak.json')  # 配置文件

PIKPAK_PATH = CONFIG['pikpak_path']  # 客户端路径
PIKPAK_CONFIG = CONFIG['pikpak_config']  # 配置文件路径
MAGNET_PATH = CONFIG['magnet_path']  # 磁链前缀


def add_to_pikpak(source: str) -> None:
    """
    获取 JSON 或 LOG 文件中的下载链接，添加到 pikpak 离线

    :param source: 来源目录
    :return: 无
    """
    # 遍历文件夹
    for root, dirs, files in os.walk(source):
        # 建立导演目录
        director = os.path.basename(root)
        if director != "Chrome":
            command = f'{PIKPAK_PATH} --config {PIKPAK_CONFIG} new folder "{director}"'
            subprocess.run(command, shell=True)

        for file_name in files:
            # 拼接出完整路径
            file_path = os.path.join(root, file_name)
            # 去掉扩展名，得到文件名字段
            file_name_no_ext = os.path.splitext(file_name)[0]
            if len(file_name_no_ext) > 100:
                file_name_no_ext = file_name_no_ext[:100]

            # 判断是否为 .json 文件
            if file_name.endswith('.json'):
                # 读取 json 文件，获取下载链接
                # dl_info = {t['size_bytes']: t['hash'] for t in read_json_to_dict(file_path)['data']['movie']['torrents']}
                # dl_link = f"{MAGNET_PATH}{dl_info[max(dl_info.keys())]}"
                dl_link = select_yts_best_torrent(read_json_to_dict(file_path))
            elif file_name.endswith('.log'):
                # 读取 log 文件，获取下载链接
                dl_link = read_file_to_list(file_path)[0]
                dl_link = dl_link.replace('\ufeff', '')
            else:
                continue

            command = f'{PIKPAK_PATH} --config {PIKPAK_CONFIG} new url -p "/{director}" -n "{file_name_no_ext}" "{dl_link}"'
            logger.info(command)
            subprocess.run(command, shell=True)
