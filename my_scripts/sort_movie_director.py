"""
从 TMDB，IMDB，豆瓣 获取导演别名，并以空文件储存名字。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
from typing import Dict

import requests
from bs4 import BeautifulSoup

from sort_movie_ops import scan_ids, merge_and_dedup, split_director_name, create_aka_director, fix_douban_name
from sort_movie_request import get_tmdb_director_details, get_imdb_director_response, get_douban_director_response

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()


def sort_movie_director(path: str) -> int:
    """
    从 TMDB，IMDB，豆瓣 抓取导演信息，生成别名文件

    :param path: 导演目录
    :return: 返回链接处理成功的数量
    """
    print("开始抓取导演信息")
    path = path.strip()
    # 初始化变量
    director_ids = scan_ids(path)
    director_info = {"country": [], "aka": []}
    director_info["aka"].append(os.path.basename(path))
    done = 0

    # TMDB 流程
    tmdb = director_ids['tmdb']
    if tmdb:
        tmdb_info = get_tmdb_director_info(tmdb)
        director_info = merge_and_dedup(director_info, tmdb_info)
        done += 1
        print("TMDB 名字：", tmdb_info.get("aka", [])[0])
    else:
        print("没有 TMDB 编号。")

    # IMDB 流程
    imdb = director_ids['imdb']
    if imdb:
        imdb_info = get_imdb_director_info(imdb)
        director_info = merge_and_dedup(director_info, imdb_info)
        done += 1
        print("IMDB 名字：", imdb_info.get("aka", [])[0])
    else:
        print("没有 IMDB 编号。")

    # DOUBAN 流程
    douban = director_ids['douban']
    if douban:
        douban_info = get_douban_director_info(douban)
        director_info = merge_and_dedup(director_info, douban_info)
        done += 1
        print("DOUBAN 名字：", douban_info.get("aka", [])[0])
    else:
        print("没有 DOUBAN 编号。")

    # 将别名写入到空文件
    aka = director_info["aka"]
    if aka:
        create_aka_director(path, aka)

    return done


def get_tmdb_director_info(director_id: str) -> Dict[str, list]:
    """
    从 TMDB 获取导演信息

    :param director_id: 导演 tmdb 编号
    :return: 返回一个字典，包含别名和国别
    """
    director_info = {"country": [], "aka": []}
    p = get_tmdb_director_details(director_id)
    director_info["aka"].append(p["name"])
    director_info["aka"].extend(list(p["also_known_as"]))
    country = p["place_of_birth"]
    if country:
        director_info["country"].append(country)
    return director_info


def get_imdb_director_info(director_id: str) -> Dict[str, list]:
    """
    从 IMDB 获取导演信息

    :param director_id: 导演 imdb 编号
    :return: 返回一个字典，包含别名和国别
    """
    director_info = {"country": [], "aka": []}
    response = get_imdb_director_response(director_id)
    if not response:
        return director_info

    soup = BeautifulSoup(response.text, 'html.parser')
    # 只获取主名字，别名很难获取，放弃
    title_tag = soup.title
    if title_tag:
        director_info["aka"].append(title_tag.string.replace(" - IMDb", ""))

    # 获取出生地信息
    birth_place_tag = soup.find('a', href=lambda x: x and 'ref_=nm_pdt_bth_loc' in x)
    if birth_place_tag:
        director_info["country"].append(birth_place_tag.get_text(strip=True))

    return director_info


def get_douban_director_info(director_id: str) -> Dict[str, list]:
    """
    从 DOUBAN 获取导演信息

    :param director_id: 导演 douban 编号
    :return: 返回一个字典，包含别名和国别
    """
    director_info = {"country": [], "aka": []}
    response = get_douban_director_response(director_id)
    if not response:
        return director_info

    soup = BeautifulSoup(response.text, 'html.parser')
    # 定位到 class 为 "subject-name" 的 h1 标签
    h1_tag = soup.find('h1', class_='subject-name')
    name_main = h1_tag.get_text(strip=True)
    name_main_list = split_director_name(name_main)
    director_info["aka"].extend(name_main_list)

    # 查找所有 <span class="label"> 标签
    labels = soup.find_all('span', class_='label')

    # 获取别名信息
    for label in labels:
        # 获取标签内文本，并去除空白字符
        label_text = label.get_text(strip=True)
        if label_text == "更多外文名:" or label_text == "更多中文名:":
            # 找到紧跟在 label 后面的 <span class="value"> 标签（也可以用 label.find_next_sibling）
            value_tag = label.find_next_sibling('span', class_='value')
            if value_tag:
                # 获取文本并去除两端空格，然后按 "/" 分割并去除分割后每个名字的前后空白
                alias_text = value_tag.get_text(strip=True)
                alias = [fix_douban_name(name) for name in alias_text.split('/') if name.strip()]
                director_info["aka"].extend(alias)
            break

    # 获取出生地信息
    for label in labels:
        label_text = label.get_text(strip=True)
        if label_text == "出生地:":
            value_tag = label.find_next_sibling('span', class_='value')
            if value_tag:
                director_info["country"].append(value_tag.get_text(strip=True))
            break

    return director_info
