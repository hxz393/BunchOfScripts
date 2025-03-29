"""
去 kpk 站点搜索下载

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
from collections import defaultdict

import requests

from sort_movie_request import get_kpk_search_response, get_kpk_page_details

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
