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
import csv
import logging
import os
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Optional

from bs4 import BeautifulSoup

from sort_movie_mysql import query_imdb_title_directors, insert_movie_wanted, remove_existing_tmdb_ids
from sort_movie_ops import (
    scan_ids,
    split_director_name,
    create_aka_director,
    fix_douban_name,
    extract_imdb_id,
    check_local_torrent,
    touch_id_marker,
)
from sort_movie_request import (
    get_tmdb_director_details,
    get_tmdb_search_response,
    get_tmdb_movie_details,
    get_douban_response,
    get_douban_search_details,
    check_kpk_for_better_quality,
    log_jackett_search_results,
    get_tmdb_director_movies,
)

logger = logging.getLogger(__name__)
WORKERS = 8
CSV_HEADERS = ["year", "imdb", "tmdb", "runtime", "titles"]


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
            touch_id_marker(path, director_id, suffix)

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
    directors = query_imdb_title_directors(movie_id)

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
    return p["name"], *list(p["also_known_as"])


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


def get_director_movies(source: str) -> Optional[List[str]]:
    """
    请求 TMDB API，获取导演所有电影列表，存成 CSV。

    :param source: 导演目录路径
    :return: imdb 列表
    """
    director_main = os.path.basename(source)
    logger.info(f"开始收集：{director_main}")
    output_csv = Path(source) / "movies.csv"
    if output_csv.exists():
        logger.info(f"已收集跳过：{director_main}")
        return None

    results_sorted = get_tmdb_director_movies_all(source)
    if not results_sorted:
        logger.info(f"没有电影跳过：{director_main}")
        return None

    imdb_ids = write_movies_csv(output_csv, results_sorted)
    logger.info(f"收集完成：{director_main}，已写入 {output_csv}")
    return imdb_ids


def write_movies_csv(output_csv: Path, movies: List[dict]) -> List[str]:
    """
    将电影列表写入 CSV，并返回其中的 IMDb 编号。

    :param output_csv: 输出文件路径
    :param movies: 电影列表
    :return: imdb 编号列表
    """
    imdb_ids = []
    with open(output_csv, mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(CSV_HEADERS)
        for item in movies:
            writer.writerow(format_movie_row(item))
            if item["imdb"]:
                imdb_ids.append(item["imdb"])

    return imdb_ids


def format_movie_row(item: dict) -> List[str]:
    """
    将单部电影信息格式化为 CSV 行。

    :param item: 电影信息
    :return: CSV 行
    """
    return [
        f"{item['year']}年" if item["year"] else "无年份",
        f"{item['imdb']}.imdb" if item["imdb"] else "无编号",
        f"{item['tmdb']}.tmdb" if item["tmdb"] else "无编号",
        f"{item['runtime']}分钟" if item["runtime"] else "无时长",
        str(item["titles"]),
    ]


def get_tmdb_director_movies_all(source: str, skip_existing: bool = False) -> Optional[list]:
    """
    从 tmdb 获取导演所有电影列表。

    :param source: 导演目录路径
    :param skip_existing: 是否跳过数据库已有数据
    :return: 电影列表
    """
    director_main = os.path.basename(source)
    tmdb_id = get_director_tmdb_id(source)
    if not tmdb_id:
        return None

    movie_ids = collect_director_movie_ids(tmdb_id, director_main, skip_existing)
    if not movie_ids:
        return None

    results = fetch_director_movies(movie_ids, director_main)
    if not results:
        logger.warning(f"没有抓取到任何有效的电影信息: {director_main}")
        return None

    # 年份为空或不是纯数字时统一排到最后，避免异常值打乱导演作品时间顺序。
    return sorted(results, key=lambda item: int(item["year"]) if str(item["year"]).isdigit() else 999999)


def get_director_tmdb_id(source: str) -> Optional[str]:
    """
    扫描导演目录中的 tmdb 编号。

    :param source: 导演目录路径
    :return: tmdb 编号
    """
    director_main = os.path.basename(source)
    director_ids = scan_ids(source)
    tmdb_id = director_ids.get("tmdb")
    if not tmdb_id:
        logger.error(f"导演没有找到 tmdb 编号：{director_main} ")
        return None

    return tmdb_id


def collect_director_movie_ids(tmdb_id: str, director_main: str, skip_existing: bool = False) -> Optional[set]:
    """
    获取导演担任导演职位的全部电影 tmdb 编号。

    :param tmdb_id: 导演 tmdb 编号
    :param director_main: 导演目录名
    :param skip_existing: 是否跳过数据库已有数据
    :return: 电影 tmdb 编号集合
    """
    movie_infos = get_tmdb_director_movies(tmdb_id)
    if not movie_infos:
        logger.error(f"无法从 TMDB 获取到导演信息: {director_main}")
        return None

    crew_list = movie_infos.get("crew", [])
    directing_list = [item for item in crew_list if item.get("job") == "Director"]
    if not directing_list:
        logger.info(f"导演没有任何作品: {director_main}")
        return None

    movie_ids = {str(item["id"]) for item in directing_list}
    if skip_existing:
        movie_ids = remove_existing_tmdb_ids(movie_ids)
    if not movie_ids:
        logger.info(f"导演所有电影已入库: {director_main}")
        return None

    return movie_ids


def fetch_director_movies(movie_ids: set, director_main: str) -> List[dict]:
    """
    并发抓取导演所有电影详情。

    :param movie_ids: 电影 tmdb 编号集合
    :param director_main: 导演目录名
    :return: 电影详情列表
    """
    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        future_to_id = {executor.submit(fetch_movie_info, movie_id): movie_id for movie_id in movie_ids}
        for future in future_to_id:
            info = future.result()
            if info:
                info["director"] = director_main
                results.append(info)

    return results


def fetch_movie_info(m_id: str) -> Optional[dict]:
    """
    获取单部电影信息。

    :param m_id: tmdb 电影编号
    :return: 电影信息，失败返回 None
    """
    try:
        detail = get_tmdb_movie_details(m_id)
        return build_movie_info(detail, m_id)
    except Exception as e:
        logger.exception(f"获取电影信息失败 (tmdb_id={m_id}): {e}")
        return None


def build_movie_info(detail: dict, m_id: str) -> dict:
    """
    将 TMDB 详情整理为脚本当前使用的电影信息结构。

    :param detail: TMDB 详情
    :param m_id: tmdb 电影编号
    :return: 电影信息
    """
    imdb_id = detail.get("imdb_id", "")
    release_date = detail.get("release_date") or ""
    year = release_date[:4] if release_date else ""
    runtime = detail.get("runtime", "")

    return {
        "director": "",
        "year": year,
        "imdb": imdb_id,
        "tmdb": m_id,
        "runtime": runtime,
        "titles": collect_alt_titles(detail),
    }


def collect_alt_titles(detail: dict) -> List[str]:
    """
    收集并去重电影标题和别名。

    :param detail: TMDB 详情
    :return: 去重后的标题列表
    """
    alt_titles = [detail.get("original_title", "")]

    translations_list = (detail.get("translations") or {}).get("translations", [])
    alt_titles.extend([item["data"]["title"] for item in translations_list if item.get("data", {}).get("title", "")])
    alt_titles.extend([item["title"] for item in detail.get("titles", []) if item.get("title", "")])

    return list({item.lower(): item for item in alt_titles}.values())


def collect_missing_director_movies(path: str) -> Optional[list[dict]]:
    """
    收集导演尚未入库的电影，并写入 wanted 表。

    :param path: 导演目录
    :return: 缺失电影列表，没有缺失时返回 None
    """
    missing_movies = get_tmdb_director_movies_all(path, skip_existing=True)
    if not missing_movies:
        return None

    logger.info(f"缺少电影列表：{missing_movies}")
    insert_movie_wanted(missing_movies)
    return missing_movies


def search_missing_director_movies(director_name: str, missing_movies: list[dict]) -> None:
    """
    对带 IMDb 编号的缺失电影做外部搜索和本地库存检查。

    :param director_name: 导演目录名
    :param missing_movies: 缺失电影列表
    :return: 无
    """
    # 外部搜索和本地库存检查都依赖 IMDb 编号，没有 IMDb 的条目到此为止。
    search_movies = [movie for movie in missing_movies if movie.get("imdb")]
    query_imdb_list = [movie["imdb"] for movie in search_movies]
    if not query_imdb_list:
        return

    logger.info(f"查询列表：{query_imdb_list}")
    logger.info("-" * 55)

    for movie in search_movies:
        imdb = movie["imdb"]
        # 这里的 titles[0] 依赖前面的电影信息整理流程：build_movie_info() 至少会放入一个标题。
        logger.info(f"标题：{movie['year']} - {movie['titles'][0]}")

        # 这里的 240p 不是实际画质，只用于让 KPK 查找更高质量资源。
        quality = "240p"
        # 这里只做搜索和日志记录，不根据返回值提前中断后续本地库存检查。
        check_kpk_for_better_quality(imdb, quality)
        log_jackett_search_results(imdb)
        local_check = check_local_torrent(imdb)
        move_counts = local_check["move_counts"]
        if move_counts:
            logger.warning(f"{imdb} 已移动本地库存种子，请检查: {move_counts}")
            time.sleep(0.1)
        logger.info("-" * 35 + director_name + "-" * 35)
        time.sleep(0.1)


def achieve_director(path: str) -> None:
    """
    为导演目录补齐缺失电影的后续查找工作。

    当前流程是：
    1. 收集导演尚未入库的电影列表，并写入 wanted 表。
    2. 仅对带 IMDb 编号的条目继续做外部搜索和本地库存检查。

    :param path: 导演目录
    :return: 无
    """
    director_name = os.path.basename(path)
    logger.info(f"处理导演归档：{director_name}")
    time.sleep(0.1)
    missing_movies = collect_missing_director_movies(path)
    if not missing_movies:
        return
    search_missing_director_movies(director_name, missing_movies)
