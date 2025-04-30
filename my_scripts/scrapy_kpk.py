"""
去 kpk 站点搜索下载

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import sys
import time
from collections import defaultdict

import requests

from sort_movie_request import get_kpk_search_response, get_kpk_page_details, get_jeckett_search_response

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()


def scrapy_kpk(imdb: str, quality: str) -> bool:
    """
    通过 IMDB 编号去搜索。

    :param imdb: IMDB 编号
    :param quality: 当前视频质量
    :return: 有更好质量时返回 True
    """
    # 获取结果 id
    logger.info(f"搜索科普库：{imdb}")
    ids = get_kpk_search_response(imdb)
    if not ids:
        logger.info(f"科普库没有结果：{imdb}")
        return False
    logger.debug(ids)

    # 获取下载信息
    merged_dict = defaultdict(list)
    for i in ids:
        r = get_kpk_page_details(i)
        if r:
            for key, value in r.items():
                merged_dict[key].extend(value)
    merged_dict = dict(merged_dict)
    if not merged_dict:
        logger.info(f"科普库没有结果：{imdb}")
        return False
    logger.debug(merged_dict)

    # 检查是否要提示
    quality_mapping = {
        "720p": ['4K/2160P', "1080P"],
        "480p": ['4K/2160P', "1080P", "720P"],
        "240p": []  # 240p 始终提示更高质量，不用检查
    }
    if quality == "240p" or any(merged_dict.get(q) for q in quality_mapping.get(quality, [])):
        logger.warning(f"{imdb} 有更高质量 {ids}：{merged_dict}")
        return True
    else:
        logger.info(f"{imdb} 无更高质量：{merged_dict}")
        return False


def scrapy_jeckett(imdb: str) -> None:
    """
    通过 IMDB 编号去搜索。

    :param imdb: IMDB 编号
    :return: 无
    """
    # 获取结果 id
    logger.info(f"搜索夹克衫：{imdb}")
    response_list = get_jeckett_search_response(imdb)
    if not response_list:
        return

    logger.warning(f"Jackett 搜索结果为：{len(response_list)}")
    time.sleep(0.1)
    extract_and_sort_data(response_list)


def bytes_to_readable(size_bytes: str) -> str:
    """
    将大小（比特）转换为可读的大小

    :param size_bytes: 大小，比特
    :return: 可读的大小
    """
    size_bytes = int(size_bytes)
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB")
    i = 0
    while size_bytes >= 1024 and i < len(size_name) - 1:
        size_bytes /= 1024.
        i += 1
    return f"{size_bytes:.2f} {size_name[i]}"


# 主函数：处理数据
def extract_and_sort_data(data: list) -> None:
    """
    解析夹克衫返回列表，记录结果

    :param data: 结果列表
    :return: 无
    """
    extracted_data = []

    for item in data:
        title = item.get('title', 'No Title')
        jackettindexer = item.get('jackettindexer', {})
        source_id = jackettindexer.get('@id', 'Unknown')
        size = item.get('size', '0')

        readable_size = bytes_to_readable(size)

        extracted_data.append({
            'title': title,
            'source_id': source_id,
            'size_bytes': int(size),
            'readable_size': readable_size
        })

    # 按大小降序排列
    extracted_data.sort(key=lambda x: x['size_bytes'], reverse=True)

    for item in extracted_data[:5]:
        logger.info(f"{item['title']} | 来源: {item['source_id']} | 大小: {item['readable_size']}")
