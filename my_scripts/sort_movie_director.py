"""
自动整理导演目录。

主流程是：
1. 从导演目录下的文件名中收集影片 IMDb 编号 `tt...`
2. 结合本地 IMDb 镜像、TMDB、豆瓣，解析导演编号
3. 抓取导演别名
4. 在导演目录中写入 `.imdb/.tmdb/.douban` 编号空文件和别名空文件
5. 如果拿到了 TMDB 编号，则将目录移动到目标位置

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
from typing import Optional

from bs4 import BeautifulSoup

from sort_movie_mysql import query_imdb_local_director
from sort_movie_ops import scan_ids, split_director_name, create_aka_director, fix_douban_name, extract_imdb_id
from sort_movie_request import (
    get_tmdb_director_details,
    get_tmdb_search_response,
    get_tmdb_movie_details,
    get_douban_response,
    get_douban_search_details,
)

logger = logging.getLogger(__name__)


def sort_director_auto(path: str, dst_path: str = r'A:\0b.导演别名') -> None:
    """
    自动整理单个导演目录。

    这个函数只负责流程编排：收集 `tt...`、解析导演编号、汇总别名、
    写入空文件，并在满足条件时移动目录。

    :param path: 导演目录
    :param dst_path: 目标目录
    :return: 无
    """
    director_main = os.path.basename(path)
    logger.info(f"开始处理：{director_main}")
    imdb_list = sorted({
        imdb_id
        for item in Path(path).rglob('*')
        if (imdb_id := extract_imdb_id(str(item)))
    })
    if not imdb_list:
        logger.error(f"目录内没有收集到 IMDB 编号: {director_main}")
        time.sleep(0.5)
        return

    # --- 搜索导演编号 ---
    logger.info("========== 搜索导演编号 ==========")
    nm_id, tmdb_id, douban_id = get_director_ids(path, director_main, imdb_list)
    if not nm_id:
        return

    # --- 获取导演别名 ---
    logger.info("========== 获取导演别名 ==========")
    aka = get_director_aliases(director_main, tmdb_id, douban_id)

    for director_id, suffix in ((nm_id, 'imdb'), (tmdb_id, 'tmdb'), (douban_id, 'douban')):
        if director_id:
            Path(path, f"{director_id}.{suffix}").touch()

    create_aka_director(path, aka)
    if tmdb_id:
        shutil.move(path, os.path.join(dst_path, director_main))


def get_director_ids(path: str, director_main: str, imdb_list: list[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    获取导演的 IMDb、TMDB、豆瓣编号。

    IMDb 导演编号是必需项；TMDB 和豆瓣编号在当前流程中是可选补充项，
    找不到时会记日志，但不会在这里中断流程。

    :param path: 导演目录
    :param director_main: 当前导演目录名
    :param imdb_list: 从目录内容中收集到的影片 IMDb 编号列表
    :return: `(nm_id, tmdb_id, douban_id)`，其中任意项都可能为 `None`
    """
    director_ids = scan_ids(path)

    # 先确定 IMDb 导演编号，后面的 TMDB 和豆瓣搜索都依赖它。
    nm_id = director_ids['imdb']
    if not nm_id:
        for imdb_id in imdb_list:
            nm_id = get_imdb_local_director(imdb_id, director_main)
            if nm_id:
                break
    else:
        logger.info(f"IMDB 编号：{nm_id}")
    if not nm_id:
        logger.error(f"IMDB 电影导演不匹配或没有导演 {director_main}")
        return None, None, None

    tmdb_id = director_ids['tmdb']
    if not tmdb_id:
        tmdb_id = get_tmdb_director_id(nm_id, director_main, imdb_list)
    else:
        logger.info(f"TMDB 编号：{tmdb_id}")
    if not tmdb_id:
        logger.error(f"没有在 tmdb 找到导演链接 {director_main}")

    douban_id = director_ids['douban']
    if not douban_id:
        douban_id = get_douban_director_id(nm_id)
    else:
        logger.info(f"DOUBAN 编号：{douban_id}")
    if not douban_id:
        logger.error(f"没有在 douban 找到导演链接 {director_main}")

    return nm_id, tmdb_id, douban_id


def get_director_aliases(director_main: str, tmdb_id: Optional[str], douban_id: Optional[str]) -> list[str]:
    """
    汇总导演别名列表。

    列表总是先放当前目录名，再按顺序追加 TMDB 和豆瓣返回的别名。

    :param director_main: 当前导演目录名
    :param tmdb_id: 导演 TMDB 编号
    :param douban_id: 导演豆瓣编号
    :return: 去重前的别名列表
    """
    aka = [director_main]

    if tmdb_id:
        tmdb_aka = get_tmdb_director_aliases(tmdb_id)
        aka.extend(tmdb_aka)
        logger.info(f"TMDB 名字：{tmdb_aka[0]}")
    else:
        logger.warning("没有 TMDB 编号。")

    if douban_id:
        douban_aka = get_douban_director_aliases(douban_id)
        aka.extend(douban_aka)
        logger.info(f"DOUBAN 名字：{douban_aka[0]}")
    else:
        logger.warning("没有 DOUBAN 编号。")

    return aka


def get_imdb_local_director(movie_id: str, director_main: str) -> Optional[str]:
    """
    根据影片 IMDb 编号，从本地 IMDb 镜像中查导演编号。

    这里不是按导演名直接查人物表，而是先用影片 `tt...` 查该片导演，
    再拿当前目录名做一次名字匹配。

    :param movie_id: imdb 编号
    :param director_main: 导演主要名字
    :return: 搜索结果，成功则返回导演编号，失败返回 None
    """
    directors = query_imdb_local_director(movie_id)

    if not directors:
        logger.error(f"IMDb 本地库没有找到导演！{movie_id} {director_main}")
        return None

    for d in directors:
        name = d.get("director_name")
        nm_id = d.get("director_id")
        if name and director_main.lower() in name.lower():
            return nm_id
        else:
            logger.warning(f"没有匹配到导演，查询到导演名字：{name}")

    return None


def get_tmdb_director_id(nm_id: str, director_main: str, imdb_list: list) -> Optional[str]:
    """
    从 TMDB 获取导演编号。

    先直接用 IMDb 导演编号 `nm...` 搜 TMDB 人物结果；
    如果没有结果，再退回到影片维度，用影片 `tt...` 找到 TMDB 电影，
    再从电影主创里反推导演。

    :param nm_id: nm 导演编号
    :param director_main: 导演主要名字
    :param imdb_list: 用于备用查询的影片 IMDb 编号列表
    :return: 搜索结果，成功则返回导演编号，失败返回 None
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
        return str(tmdb_id) if tmdb_id else None
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
                    })
            # 尝试匹配 director_main
            for d in directors:
                if d["name"] and d["name"].lower() == director_main.lower():
                    return str(d["name_id"]) if d["name_id"] else None
                tmdb_id = str(d["name_id"]) if d["name_id"] else None
                # 名字不完全相等时，再查一次导演详情，用 TMDB 的 alias 判断是不是同一个人。
                aliases = get_tmdb_director_aliases(tmdb_id) if tmdb_id else ()
                director_main_normalized = director_main.lower().replace(" ", "")
                aliases_normalized = {name.lower().replace(" ", "") for name in aliases}
                if tmdb_id and director_main_normalized in aliases_normalized:
                    return tmdb_id
                logger.warning(f"没有匹配到导演，查询到导演名字：{list(aliases)} {tmdb_id}")
    return


def get_douban_director_id(nm_id: str) -> Optional[str]:
    """
    根据 IMDb 导演编号搜索豆瓣导演编号。

    :param nm_id: imdb 导演编号
    :return: 搜索结果，成功则返回导演编号，失败返回 None
    """
    # 搜索编号，获取结果
    r = get_douban_response(nm_id, "director_search")
    if not r:
        logger.error("没有获取到豆瓣搜索响应")
        return

    inner_url = get_douban_search_details(r)
    if not inner_url:
        return None

    m = re.search(r'/personage/(\d+)', inner_url)
    if m:
        return m.group(1)

    logger.warning(f"豆瓣导演链接无法解析编号：{inner_url}")
    return None


def get_tmdb_director_aliases(director_id: str) -> tuple[str, ...]:
    """
    从 TMDB 获取导演别名。

    返回值第一个元素是 TMDB 主名字，后面依次是 `also_known_as`
    中的别名。

    :param director_id: 导演 tmdb 编号
    :return: 返回别名序列
    """
    p = get_tmdb_director_details(director_id)
    return (p["name"], *list(p["also_known_as"]))


def get_douban_director_aliases(director_id: str) -> list:
    """
    从豆瓣人物页提取导演别名。

    返回值先放豆瓣页面主标题拆出来的名字，再追加“更多外文名/更多中文名”
    里的条目。

    :param director_id: 导演 douban 编号
    :return: 返回别名列表
    """
    response = get_douban_response(director_id, "director_response")
    if not response:
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    # 定位到 class 为 "subject-name" 的 h1 标签
    h1_tag = soup.find('h1', class_='subject-name')
    name_main = h1_tag.get_text(strip=True)
    name_main_list = split_director_name(name_main)
    aka = list(name_main_list)

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
                aka.extend(alias)
            break

    return aka
