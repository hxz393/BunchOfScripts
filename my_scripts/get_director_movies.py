"""
从 tmdb 获取导演所有电影信息，存到文本文件中

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import csv
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, List

from sort_movie_mysql import remove_existing_tmdb_ids
from sort_movie_ops import scan_ids
from sort_movie_request import get_tmdb_director_movies, get_tmdb_movie_details

logger = logging.getLogger(__name__)

WORKERS = 8
CSV_HEADERS = ["year", "imdb", "tmdb", "runtime", "titles"]


def get_director_movies(source: str) -> Optional[List[str]]:
    """
    请求 TMDB API，获取导演所有电影列表，存成 CSV

    :param source: 导演目录路径
    :return: imdb 列表
    """
    # 检查是否已经抓取过
    director_main = os.path.basename(source)
    logger.info(f"开始收集：{director_main}")
    p = Path(source)
    output_csv = p / 'movies.csv'
    if output_csv.exists():
        logger.info(f"已收集跳过：{director_main}")
        return

    # 获取所有电影列表
    results_sorted = get_tmdb_director_movies_all(source)
    if not results_sorted:
        logger.info(f"没有电影跳过：{director_main}")
        return

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
    with open(output_csv, mode='w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)
        for item in movies:
            writer.writerow(format_movie_row(item))
            if item['imdb']:
                imdb_ids.append(item['imdb'])

    return imdb_ids


def format_movie_row(item: dict) -> List[str]:
    """
    将单部电影信息格式化为 CSV 行。

    :param item: 电影信息
    :return: CSV 行
    """
    return [
        f"{item['year']}年" if item['year'] else '无年份',
        f"{item['imdb']}.imdb" if item['imdb'] else '无编号',
        f"{item['tmdb']}.tmdb" if item['tmdb'] else '无编号',
        f"{item['runtime']}分钟" if item['runtime'] else '无时长',
        str(item['titles'])
    ]


def get_tmdb_director_movies_all(source: str, skip_existing: bool = False) -> Optional[list]:
    """
    从 tmdb 获取导演所有电影列表

    :param source: 导演目录路径
    :param skip_existing: 是否跳过数据库已有数据
    :return: 电影列表
    """
    # 获取 tmdb 编号
    director_main = os.path.basename(source)
    tmdb_id = get_director_tmdb_id(source)
    if not tmdb_id:
        return

    movie_ids = collect_director_movie_ids(tmdb_id, director_main, skip_existing)
    if not movie_ids:
        return

    results = fetch_director_movies(movie_ids, director_main)
    if not results:
        logger.warning(f"没有抓取到任何有效的电影信息: {director_main}")
        return

    return sort_movies_by_year(results)


def get_director_tmdb_id(source: str) -> Optional[str]:
    """
    扫描导演目录中的 tmdb 编号。

    :param source: 导演目录路径
    :return: tmdb 编号
    """
    director_main = os.path.basename(source)
    director_ids = scan_ids(source)
    tmdb_id = director_ids.get('tmdb')
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

    crew_list = movie_infos.get('crew', [])
    directing_list = [item for item in crew_list if item.get('job') == 'Director']
    if not directing_list:
        logger.info(f"导演没有任何作品: {director_main}")
        return None

    movie_ids = {str(item['id']) for item in directing_list}
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
        future_to_id = {executor.submit(fetch_movie_info, m_id): m_id for m_id in movie_ids}
        for future in future_to_id:
            info = future.result()
            if info:
                info["director"] = director_main
                results.append(info)

    return results


def sort_movies_by_year(movies: List[dict]) -> List[dict]:
    """
    按年份升序排序电影列表，没有年份的排在最后。

    :param movies: 电影列表
    :return: 排序后的电影列表
    """
    return sorted(movies, key=lambda item: year_to_int(item['year']))


def year_to_int(year: str) -> int:
    """
    将年份字符串转为数字，空值或异常值排到最后。

    :param year: 年份字符串
    :return: 排序用年份
    """
    try:
        return int(year)
    except ValueError:
        return 999999


def fetch_movie_info(m_id: str) -> Optional[dict]:
    """
    获取单部电影信息

    :param m_id: tmdb 电影编号
    :return: None
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
    imdb_id = detail.get('imdb_id', '')
    release_date = detail.get('release_date') or ''
    year = release_date[:4] if release_date else ''
    runtime = detail.get('runtime', '')

    return {
        'director': '',
        'year': year,
        'imdb': imdb_id,
        'tmdb': m_id,
        'runtime': runtime,
        'titles': collect_alt_titles(detail)
    }


def collect_alt_titles(detail: dict) -> List[str]:
    """
    收集并去重电影标题和别名。

    :param detail: TMDB 详情
    :return: 去重后的标题列表
    """
    alt_titles = [detail.get('original_title', '')]

    translations_list = (detail.get('translations') or {}).get('translations', [])
    alt_titles.extend([item['data']['title'] for item in translations_list if item.get('data', {}).get('title', '')])
    alt_titles.extend([item['title'] for item in detail.get('titles', []) if item.get('title', '')])

    return list({item.lower(): item for item in alt_titles}.values())
