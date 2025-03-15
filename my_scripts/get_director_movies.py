"""
从 tmdb 获取导演所有电影信息，存到文本文件中

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import csv
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

from sort_movie_ops import scan_ids
from sort_movie_request import get_tmdb_director_movies, get_tmdb_movie_details

logger = logging.getLogger(__name__)

WORKERS = 16


def get_director_movies(source: str) -> None:
    """
    请求 TMDB API，获取导演所有电影列表，存成 CSV

    :param source: 导演目录路径
    :return: None
    """
    # 检查是否已经抓取过
    director_main = os.path.basename(source)
    logger.info(f"开始收集：{director_main}")
    p = Path(source)
    output_csv = p / 'movies.csv'
    if output_csv.exists():
        return

    # 获取 tmdb 编号
    director_ids = scan_ids(source)
    tmdb_id = director_ids.get('tmdb')
    if not tmdb_id:
        logger.error(f"没有找到 tmdb 编号：{director_main} ")
        return

    # 从 TMDB 获取导演相关电影信息
    movie_infos = get_tmdb_director_movies(tmdb_id)
    if not movie_infos:
        logger.error(f"无法从 TMDB 获取到任何电影信息: {director_main}")
        return

    # 筛选 department = 'Directing' 的条目
    crew_list = movie_infos.get('crew', [])
    directing_list = [item for item in crew_list if item.get('department') == 'Directing']
    if not directing_list:
        logger.info(f"没有在 {director_main} 找到导演作品")
        return

    # 取所有电影 id 并去重
    movie_ids = {item['id'] for item in directing_list}
    # 使用多线程加速对每部电影的抓取
    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        future_to_id = {executor.submit(fetch_movie_info, m_id): m_id for m_id in movie_ids}
        for future in future_to_id:
            info = future.result()
            if info:
                results.append(info)

    if not results:
        logger.warning(f"没有抓取到任何有效的电影信息: {director_main}")
        return

    # 对结果排序
    def year_to_int(y):
        """辅助函数，转换年份"""
        try:
            return int(y)
        except ValueError:
            return 999999  # 没有年份时排序在后

    results_sorted = sorted(results, key=lambda x: year_to_int(x['year']))

    # 将结果写入 CSV
    with open(output_csv, mode='w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        # 写表头
        writer.writerow(["year", "imdb_id", "tmdb_id", "runtime", "titles"])
        # 写内容
        for item in results_sorted:
            writer.writerow([
                f"{item['year']}年" if item['year'] else '无年份',
                f"{item['imdb_id']}.imdb" if item['imdb_id'] else '无编号',
                f"{item['tmdb_id']}.tmdb" if item['tmdb_id'] else '无编号',
                f"{item['runtime']}分钟" if item['runtime'] else '无时长',
                item['titles']
            ])

    logger.info(f"收集完成：{director_main}，已写入 {output_csv}")


def fetch_movie_info(m_id: str) -> Optional[dict]:
    """
    获取单部电影信息

    :param m_id: tmdb 电影编号
    :return: None
    """
    try:
        detail = get_tmdb_movie_details(m_id)

        imdb_id = detail.get('imdb_id', '')
        release_date = detail.get('release_date') or ''
        year = release_date[:4] if release_date else ''
        runtime = detail.get('runtime', '')
        alt_titles = [detail.get('original_title', '')]

        # 收集所有别名，以分号拼接为字符串
        translations_list = detail.get('translations', {}).get('translations', [])
        alt_titles.extend([item['data']['title'] for item in translations_list if item.get('data', {}).get('title', '')])
        all_alias = " ;".join(alt_titles)

        return {
            'year': year,
            'imdb_id': imdb_id,
            'tmdb_id': m_id,
            'runtime': runtime,
            'titles': all_alias
        }
    except Exception as e:
        logger.exception(f"获取电影信息失败 (tmdb_id={m_id}): {e}")
        return None
