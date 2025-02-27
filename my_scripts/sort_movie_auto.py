"""
自动化整理电影和导演文件夹

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Optional

import requests
from retrying import retry

from my_module import read_file_to_list, write_list_to_file
from sort_movie import sort_movie
from sort_movie_director import sort_movie_director
from sort_movie_ops import get_ids, safe_get, scan_ids
from sort_movie_request import get_imdb_movie_details, get_tmdb_search_response, get_tmdb_director_details, get_douban_search_response, get_douban_search_details, get_tmdb_movie_details
from sort_ru import ru_search

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()


def sort_director_auto(path: str) -> None:
    """
    自动整理导演目录，生成导演别名空文件

    :param path: 导演目录
    :return: 无
    """
    # 查找 imdb 编号
    result_list = [path]
    director_main = os.path.basename(path)
    print(f"开始处理：{director_main}")
    file_list = os.listdir(path)
    imdb_list = []
    for file_path in file_list:
        imdb_id = m.group(1) if (m := re.search(r'(tt\d+)', file_path)) else None
        if imdb_id:
            imdb_list.append(imdb_id)
    if not imdb_list:
        logger.error(f"没有找到 IMDB 编号: {director_main}")
        time.sleep(0.5)
        return
    imdb_list = list(set(imdb_list))
    director_ids = scan_ids(path)

    # 搜索 imdb，获取导演链接
    nm_id = director_ids['imdb']
    if not nm_id:
        for imdb_id in imdb_list:
            r = get_imdb_director(imdb_id, director_main)
            if r:
                result_list.append(r)
                nm_id = m.group(1) if (m := re.search(r'(nm\d+)', r)) else None
                break
    else:
        print(f"IMDB 编号：{nm_id}")
    if not nm_id:
        logger.error(f"没有在 imdb 找到导演链接 {director_main}")
        return

    # 搜索 tmdb，获取导演链接
    tmdb_id = director_ids['tmdb']
    if not tmdb_id:
        tmdb_id = get_tmdb_director(nm_id, director_main, imdb_list)
        if tmdb_id:
            result_list.append(tmdb_id)
    else:
        print(f"TMDB 编号：{tmdb_id}")
    if not tmdb_id:
        logger.error(f"没有在 tmdb 找到导演链接 {director_main}")

    # 搜索 douban，获取导演链接
    douban_id = director_ids['douban']
    if not douban_id:
        douban_id = get_douban_director(nm_id)
        if douban_id:
            result_list.append(douban_id)
    else:
        print(f"DOUBAN 编号：{douban_id}")
    if not douban_id:
        logger.error(f"没有在 douban 找到导演链接 {director_main}")

    # 将结果写入到文件，执行自动抓取
    print(f"查询结果：{result_list}\n")
    target_file = r'B:\2.脚本\!00.txt'
    write_list_to_file(target_file, result_list)
    get_ids(target_file)
    done = sort_movie_director(read_file_to_list(target_file)[0])
    if done == 3:
        shutil.move(path, os.path.join(r'A:\0b.导演别名', director_main))


def get_imdb_director(movie_id: str, director_main: str) -> Optional[str]:
    """
    搜索 imdb，获取导演信息

    :param movie_id: imdb 编号
    :param director_main: 导演主要名字
    :return: 搜索结果，成功则返回导演链接，失败返回 None
    """
    m = get_imdb_movie_details(movie_id)
    if not m:
        return

    # 获取导演列表，这个结构有点特别，需要分步处理
    directors_list = safe_get(
        m,
        ["props", "pageProps", "aboveTheFoldData", "directorsPageTitle"],
        default=[]
    )
    if len(directors_list) > 1:
        logger.error(f"导演列表有多个元素：{directors_list}")
        sys.exit(1)
    first_item = directors_list[0] if directors_list else {}
    credits_list = safe_get(first_item, ["credits"], default=[])

    # 提取列表中的导演和编号
    directors = []
    for credit in credits_list:
        name = safe_get(credit, ["name", "nameText", "text"], default="")
        nm_id = safe_get(credit, ["name", "id"], default="")
        directors.append({
            "name": name,
            "link": f"https://www.imdb.com/name/{nm_id}"
        })

    # 尝试匹配传入的导演主名字，匹配到了就返回
    for d in directors:
        if d["name"] and director_main.lower() in d["name"].lower():
            return d["link"]
        else:
            print(f"没有匹配到导演，查询到导演名字：{d['name']}")

    # 如果没有匹配到导演的名字返回 None
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
    link = f"https://www.themoviedb.org/person/{tmdb_id}"
    if director_main.lower().replace(" ", "") in aka:
        return link
    else:
        print(f"没有匹配到导演，查询到导演名字：{aka_org} {link}")
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
        return f"https://www.themoviedb.org/person/{tmdb_id}"
    else:
        print(f"没有在 TMDB 上搜索到导演：{nm_id}，尝试通过电影获取导演")

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
            # 3. 在 crew 中筛选出 job == 'Director' 的人员
            directors = []
            for member in crew_list:
                if member.get('job') == 'Director':
                    member_id = member.get('id')
                    directors.append({
                        "name_id": member_id,
                        "name": member.get('name'),
                        "link": f"https://www.themoviedb.org/person/{member_id}"
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


@retry(stop_max_attempt_number=3, wait_random_min=30, wait_random_max=300)
def get_douban_director(nm_id: str) -> Optional[str]:
    """
    搜索 douban，获取导演信息

    :param nm_id: imdb 导演编号
    :return: 搜索结果，成功则返回导演链接，失败返回 None
    """
    # 搜索编号，获取结果
    r = get_douban_search_response(nm_id, "1065")
    if r:
        return get_douban_search_details(r)


def sort_movie_auto(path: str) -> None:
    """
    自动整理电影，输出链接到文本文件

    :param path: 导演目录
    :return: 无
    """
    folders = [os.path.join(path, item) for item in os.listdir(path) if os.path.isdir(os.path.join(path, item))]
    target_file = r'B:\2.脚本\!00.txt'
    if not folders:
        logger.error("目录下没有子文件夹")
        return

    for folder in folders:
        print(f"开始处理：{folder}")
        r = sort_movie_auto_folder(folder, target_file)
        if r:
            logger.error(r)
            return
        get_ids(target_file)
        url_list = read_file_to_list(target_file)
        sort_movie(url_list[0])
        print("=" * 255 + "\n" * 2)


def sort_movie_auto_folder(path: str, target_file: str) -> Optional[str]:
    """
    自动整理电影

    :param path: 电影目录
    :param target_file: 来源文本文件
    :return: 失败时返回原因
    """
    result_list = [path]
    imdb_id = m.group(1) if (m := re.search(r'(tt\d+)', path)) else None
    if not imdb_id:
        return "目录缺少 IMDB 编号"
    imdb_url = f"https://www.imdb.com/title/{imdb_id}/"
    result_list.append(imdb_url)

    # 搜索 tmdb，获取链接
    r = get_tmdb_id(imdb_id)
    if r["result"]:
        logger.error(r["result"])
    tmdb_url = r["tmdb_url"]
    result_list.append(tmdb_url)

    # 搜索豆瓣，获取链接
    r = get_douban_id(imdb_id)
    if r["result"]:
        logger.error(r["result"])
    douban_url = r["douban_url"]
    result_list.append(douban_url)

    # 将结果写入到文件
    write_list_to_file(target_file, result_list)
    print("-" * 255)


@retry(stop_max_attempt_number=3, wait_random_min=30, wait_random_max=300)
def get_tmdb_id(imdb_id: str) -> dict:
    """
    搜索tmdb，获取 tmdb 电影链接

    :param imdb_id: imdb 编号
    :return: 搜索结果
    """
    return_dict = {"result": "", "tmdb_url": ""}
    # 搜索 tmdb，获取响应
    search_result = get_tmdb_search_response(imdb_id)
    if not search_result.get('movie_results'):
        return_dict["result"] = f"tmdb 搜索失败，没找到电影"
        return return_dict

    # 取第一条结果
    movie_id = search_result.get('movie_results')[0].get('id')
    if not movie_id:
        return_dict["result"] = f"获取 tmdb id 失败"
        return return_dict

    return_dict["tmdb_url"] = f"https://www.themoviedb.org/movie/{movie_id}"
    return return_dict


@retry(stop_max_attempt_number=3, wait_random_min=300, wait_random_max=900)
def get_douban_id(imdb_id: str) -> dict:
    """
    搜索豆瓣，获取豆瓣电影链接

    :param imdb_id: imdb 编号
    :return: 搜索结果
    """
    return_dict = {"result": "", "douban_url": ""}
    search_type = "1002"
    # 搜索豆瓣，获取响应
    response = get_douban_search_response(imdb_id, search_type)
    if not response:
        return_dict["result"] = "豆瓣电影搜索失败"
        return return_dict

    # 解析响应内容
    inner_url = get_douban_search_details(response)
    if not inner_url:
        return_dict["result"] = "豆瓣电影链接解析失败"
        return return_dict

    return_dict["douban_url"] = inner_url
    return return_dict


def sort_ru_auto(path: str) -> None:
    """
    自动搜索下载，如果有搜索结果则放弃继续搜索

    :param path: 导演目录
    :return: 无
    """
    # 获取所有名字到 aka 列表
    print(f"开始处理：{path}")
    p = Path(path)
    aka = []
    for path_item in p.iterdir():
        if path_item.is_file():
            if path_item.suffix == "":
                aka.append(path_item.name)

    for n in aka:
        name, result = ru_search(n)
        print(f"搜索关键字: {name} -> {result}")
        if result != 0:
            return
