"""
处理 yts.mx 抓取的没有导演电影，去 IMDB 上获取导演信息
使用字符串解析技术，而不是常用的解析 json

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import re
import shutil
from pathlib import Path

import requests

from my_module import read_json_to_dict
from sort_movie_request import get_tmdb_movie_details

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG = read_json_to_dict('config/scrapy_yts_fix_imdb.json')  # 配置文件

FILE_PATH = CONFIG['file_path']  # json 文件储存路径
HEADER_IMDB = CONFIG['header_imdb']  # IMDB 请求头
BASE_URL = CONFIG['base_url']  # 搜索地址


def scrapy_yts_fix_imdb() -> None:
    """
    去 IMDB 获取导演信息，并整理文件

    :return: 无
    """
    # 根据文件名获取 tt 编号
    for root, dirs, files in os.walk(FILE_PATH):
        for file_name in files:
            if file_name.endswith('.json'):
                logger.info(f"处理：{file_name}")
                file_path = Path(os.path.join(root, file_name))
                # imdb = file_name.split('{')[1].split('}')[0]
                imdb = m.group(1) if (m := re.search(r'(tt\d+)', file_name)) else None
                if not imdb:
                    logger.error(f"没有找到 tt 编号：{file_name}")
                    continue

                # 查询 IMDB
                folder_name = search_imdb(imdb)
                if not folder_name:
                    folder_name = '没有导演'

                # 查询 TMDB
                movie_details = get_tmdb_movie_details(imdb)
                if movie_details:
                    crew_list = movie_details['casts'].get('crew', [])
                    for member in crew_list:
                        if member.get('job') == 'Director':
                            folder_name = member.get('name')
                            break

                logger.info(f"导演名：{folder_name}")

                folder_path = Path(os.path.join(Path(root).parent, folder_name))
                folder_path.mkdir(parents=True, exist_ok=True)
                target_file_path = folder_path / file_path.name
                shutil.move(file_path, target_file_path)
                logger.info("*" * 255)


def search_imdb(movie_id: str) -> str:
    """
    用 tt 编号搜索 IMDb 网站，获取导演名并返回

    :param movie_id: imdb 编号
    :return: 导演名
    """
    logger.info(f"查询 IMDB：{movie_id}")
    url = f"{BASE_URL}/{movie_id}/"
    r = requests.get(url, verify=False, headers=HEADER_IMDB)
    directors_part = extract_directors_section(r.text)
    director_name = get_director_name_from_section(directors_part)
    return director_name


def extract_directors_section(big_string: str) -> str:
    """
    在大字符串中查找 "directors": [ ... ] 这一段。
    如果找到就返回这段字符串(含方括号)；找不到返回空字符串。

    :param big_string: 原始 HTML 代码
    :return: 导演名
    """
    target = '"directors":'
    start_idx = big_string.find(target)
    if start_idx == -1:
        return ""

    # 从 "directors": 之后找到第一个 '[' 的位置
    bracket_start = big_string.find('[', start_idx)
    if bracket_start == -1:
        return ""

    # 手动匹配方括号
    bracket_count = 0
    end_idx = -1
    for i in range(bracket_start, len(big_string)):
        if big_string[i] == '[':
            bracket_count += 1
        elif big_string[i] == ']':
            bracket_count -= 1
            if bracket_count == 0:
                end_idx = i  # 找到与起始 '[' 对应的 ']'
                break

    if end_idx == -1:
        # 没能成功匹配方括号
        return ""

    # 把 [ ... ] 整段截取出来
    directors_part = big_string[bracket_start:end_idx + 1]
    return directors_part


def get_director_name_from_section(directors_part: str) -> str:
    """
    在已截取到的 directors: [ ... ] 字符串中，去搜索导演姓名。
    如果搜索不到姓名就返回空字符串。

    :param directors_part: 原始 HTML 代码
    :return: 导演名
    """
    # 如果 directors_part 就是 []，代表没有导演
    if directors_part.strip() == "[]":
        return ""

    # 这里还是用最简单的字符串定位来拿:
    name_key = '"nameText":{"text":"'
    start_idx = directors_part.find(name_key)
    if start_idx == -1:
        return ""

    start_idx += len(name_key)
    end_idx = directors_part.find('"', start_idx)
    if end_idx == -1:
        return ""

    return directors_part[start_idx:end_idx]
