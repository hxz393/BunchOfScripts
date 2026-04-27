"""
从 TMDB，IMDB，豆瓣 获取导演别名，并以空文件储存名字。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import logging
import os
import re
import shutil
import time
from pathlib import Path
from typing import Dict, Optional

from bs4 import BeautifulSoup

from my_module import read_json_to_dict, write_list_to_file
from sort_movie_mysql import query_imdb_local_director
from sort_movie_ops import get_ids, scan_ids, merge_and_dedup, split_director_name, create_aka_director, fix_douban_name
from sort_movie_request import (
    get_tmdb_director_details,
    get_tmdb_search_response,
    get_tmdb_movie_details,
    get_douban_response,
    get_douban_search_details,
)

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict('config/sort_movie.json')  # 配置文件

TMDB_PERSON_URL = CONFIG['tmdb_person_url']  # tmdb 导演地址
IMDB_PERSON_URL = CONFIG['imdb_person_url']  # imdb 导演地址


def sort_director_auto(path: str, dst_path: str = r'A:\0b.导演别名') -> None:
    """
    自动整理导演目录，生成导演别名空文件

    :param path: 导演目录
    :param dst_path: 目标目录
    :return: 无
    """
    # 查找 imdb 编号
    result_list = [path]
    director_main = os.path.basename(path)
    logger.info(f"开始处理：{director_main}")
    p = Path(path)
    file_list = [str(item) for item in p.rglob('*')]
    imdb_list = []
    for file_path in file_list:
        imdb_id = m.group(1) if (m := re.search(r'(tt\d+)', file_path)) else None
        if imdb_id:
            imdb_list.append(imdb_id)
    if not imdb_list:
        logger.error(f"目录内没有收集到 IMDB 编号: {director_main}")
        time.sleep(0.5)
        return
    imdb_list = list(set(imdb_list))
    director_ids = scan_ids(path)
    director_info = {"country": [], "aka": []}

    # 搜索 imdb，获取导演链接
    nm_id = director_ids['imdb']
    if not nm_id:
        for imdb_id in imdb_list:
            r = get_imdb_local_director(imdb_id, director_main)
            if r:
                director_info["aka"].append(director_main)
                result_list.append(r)
                nm_id = m.group(1) if (m := re.search(r'(nm\d+)', r)) else None
                break
    else:
        logger.info(f"IMDB 编号：{nm_id}")
    if not nm_id:
        logger.error(f"IMDB 电影导演不匹配或没有导演 {director_main}")
        return

    # 搜索 tmdb，获取导演链接
    tmdb_id = director_ids['tmdb']
    if not tmdb_id:
        tmdb_id = get_tmdb_director(nm_id, director_main, imdb_list)
        if tmdb_id:
            result_list.append(tmdb_id)
    else:
        logger.info(f"TMDB 编号：{tmdb_id}")
    if not tmdb_id:
        logger.error(f"没有在 tmdb 找到导演链接 {director_main}")

    # 搜索 douban，获取导演链接
    douban_id = director_ids['douban']
    if not douban_id:
        douban_id = get_douban_director(nm_id)
        if douban_id:
            result_list.append(douban_id)
    else:
        logger.info(f"DOUBAN 编号：{douban_id}")
    if not douban_id:
        logger.error(f"没有在 douban 找到导演链接 {director_main}")

    # 将结果写入到文件，执行自动抓取
    logger.info(f"查询结果：{result_list}\n")
    target_file = r'B:\2.脚本\!00.txt'
    write_list_to_file(target_file, result_list)
    get_ids(target_file)
    done = sort_movie_director(path, director_info)
    if done == 2:
        shutil.move(path, os.path.join(dst_path, director_main))


def get_imdb_local_director(movie_id: str, director_main: str) -> Optional[str]:
    """
    搜索本地 imdb 库，获取导演信息

    :param movie_id: imdb 编号
    :param director_main: 导演主要名字
    :return: 搜索结果，成功则返回导演链接，失败返回 None
    """
    directors = query_imdb_local_director(movie_id)

    if not directors:
        logger.error(f"IMDb 本地库没有找到导演！{movie_id} {director_main}")
        return None

    for d in directors:
        name = d.get("director_name")
        nm_id = d.get("director_id")
        if name and director_main.lower() in name.lower():
            return f"{IMDB_PERSON_URL}/{nm_id}"
        else:
            logger.warning(f"没有匹配到导演，查询到导演名字：{name}")

    return None


def get_tmdb_director_aka(tmdb_id: str, director_main: str) -> Optional[str]:
    """获取导演详细信息，得到别名列表，然后匹配

    :param tmdb_id: tmdb 编号
    :param director_main: 导演主要名字
    :return: 匹配，成功则返回导演链接，失败返回 None
    """
    p = get_tmdb_director_details(tmdb_id)
    aka_org = list(p["also_known_as"])
    aka_org.append(p['name'])
    aka = [i.lower().replace(" ", "") for i in aka_org]
    link = f"{TMDB_PERSON_URL}/{tmdb_id}"
    if director_main.lower().replace(" ", "") in aka:
        return link
    else:
        logger.warning(f"没有匹配到导演，查询到导演名字：{aka_org} {link}")
        return None


def get_tmdb_director(nm_id: str, director_main: str, imdb_list: list) -> Optional[str]:
    """
    两种方式搜索 tmdb，获取导演信息

    :param nm_id: nm 导演编号
    :param director_main: 导演主要名字
    :param imdb_list: 通过 imdb 列表搜索电影获取导演信息，备用方式
    :return: 搜索结果，成功则返回导演链接，失败返回 None
    """
    # 搜索导演，获取导演信息
    data = get_tmdb_search_response(nm_id)
    if not data:
        return

    persons = data.get("person_results", [])
    if persons:
        # 一般只有一个结果
        person = persons[0]
        tmdb_id = person.get('id')
        return f"{TMDB_PERSON_URL}/{tmdb_id}"
    else:
        logger.warning(f"没有在 TMDB 上搜索到导演：{nm_id}，尝试通过电影获取导演")

    # 麻烦的方法
    for imdb in imdb_list:
        r = get_tmdb_search_response(imdb)
        if r:
            movie_results = r.get('movie_results', [])
            if not movie_results:
                continue
            movie_id = movie_results[0].get('id')
            movie_details = get_tmdb_movie_details(movie_id)
            crew_list = movie_details['casts'].get('crew', [])
            # 在 crew 中筛选出 job == 'Director' 的人员
            directors = []
            for member in crew_list:
                if member.get('job') == 'Director':
                    member_id = member.get('id')
                    directors.append({
                        "name_id": member_id,
                        "name": member.get('name'),
                        "link": f"{TMDB_PERSON_URL}/{member_id}"
                    })
            # 尝试匹配 director_main
            for d in directors:
                if d["name"] and d["name"].lower() == director_main.lower():
                    return d["link"]
                else:
                    result = get_tmdb_director_aka(d["name_id"], director_main)
                    if result:
                        return result
    return


def get_douban_director(nm_id: str) -> Optional[str]:
    """
    搜索 douban，获取导演信息

    :param nm_id: imdb 导演编号
    :return: 搜索结果，成功则返回导演链接，失败返回 None
    """
    # 搜索编号，获取结果
    r = get_douban_response(nm_id, "director_search")
    if not r:
        logger.error("没有获取到豆瓣搜索响应")
        return

    return get_douban_search_details(r)


def sort_movie_director(path: str, director_info) -> int:
    """
    从 TMDB，IMDB，豆瓣 抓取导演信息，生成别名文件

    :param path: 导演目录
    :param director_info: 导演信息
    :return: 返回链接处理成功的数量
    """
    logger.info("开始抓取导演信息")
    path = path.strip()
    # 初始化变量
    director_ids = scan_ids(path)
    director_info["aka"].append(os.path.basename(path))
    done = 0

    # IMDB 流程
    imdb = director_ids['imdb']
    if imdb:
        done += 1
        logger.info(f"IMDB 编号：{imdb}")
    else:
        logger.warning("没有 IMDB 编号。")

    # TMDB 流程
    tmdb = director_ids['tmdb']
    if tmdb:
        tmdb_info = get_tmdb_director_info(tmdb)
        director_info = merge_and_dedup(director_info, tmdb_info)
        done += 1
        logger.info(f"TMDB 名字：{tmdb_info.get('aka', [])[0]}")
    else:
        logger.warning("没有 TMDB 编号。")

    # DOUBAN 流程
    douban = director_ids['douban']
    if douban:
        douban_info = get_douban_director_info(douban)
        director_info = merge_and_dedup(director_info, douban_info)
        logger.info(f"DOUBAN 名字：{douban_info.get('aka', [])[0]}")
    else:
        logger.warning("没有 DOUBAN 编号。")

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


def get_douban_director_info(director_id: str) -> Dict[str, list]:
    """
    从 DOUBAN 获取导演信息

    :param director_id: 导演 douban 编号
    :return: 返回一个字典，包含别名和国别
    """
    director_info = {"country": [], "aka": []}
    response = get_douban_response(director_id, "director_response")
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
