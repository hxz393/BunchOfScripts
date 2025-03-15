"""
从 JSON 文件中读取数据，插入到数据库

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393
"""

import json
import logging
import os.path
from datetime import datetime
from typing import Any

import mysql.connector
import requests

from my_module import read_json_to_dict

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG = read_json_to_dict('config/sort_movie.json')  # 配置文件

MYSQL_HOST = CONFIG['mysql_host']  # mysql 主机地址
MYSQL_USER = CONFIG['mysql_user']  # mysql 用户名
MYSQL_PASS = CONFIG['mysql_pass']  # mysql 密码
MYSQL_DB = CONFIG['mysql_db']  # mysql 数据库

UPDATE_SQL = """
            UPDATE movies
            SET director=%s,
                year=%s,
                original_title=%s,
                chinese_title=%s,
                genres=%s,
                country=%s,
                language=%s,
                runtime=%s,
                titles=%s,
                directors=%s,
                tmdb=%s,
                douban=%s,
                imdb=%s,
                source=%s,
                quality=%s,
                resolution=%s,
                codec=%s,
                bitrate=%s,
                size=%s,
                dl_link=%s,
                updated_at=%s
            WHERE id=%s
        """
INSERT_SQL = """
            INSERT INTO movies (
                director, year, original_title, chinese_title, genres,
                country, language, runtime, titles, directors,
                tmdb, douban, imdb, source, quality, resolution,
                codec, bitrate, size, dl_link,
                created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s
            )
        """
SEARCH_SQL = "SELECT resolution, bitrate, size, tmdb, douban, imdb FROM movies WHERE id = %s"


def sort_movie_mysql(path: str) -> None:
    """
    将数据插入到 MySQL

    :param path: 电影目录
    :return: 无
    """
    merged_dict = read_json_to_dict(os.path.join(path, "movie_info.json5"))
    if not merged_dict:
        logger.error("无法读取 JSON 文件")
        return

    # 建立数据库连接
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB
    )
    cursor = conn.cursor()

    # 需要插入或更新的字段（不包括 created_at 和 updated_at）
    data_values = (
        merged_dict['director'],
        merged_dict['year'],
        merged_dict['original_title'],
        merged_dict['chinese_title'],
        json.dumps(merged_dict['genres']),
        json.dumps(merged_dict['country']),
        json.dumps(merged_dict['language']),
        merged_dict['runtime'],
        json.dumps(merged_dict['titles']),
        json.dumps(merged_dict['directors']),
        merged_dict['tmdb'],
        merged_dict['douban'],
        merged_dict['imdb'],
        merged_dict['source'],
        merged_dict['quality'],
        merged_dict['resolution'],
        merged_dict['codec'],
        merged_dict['bitrate'],
        merged_dict['size'],
        merged_dict['dl_link']
    )

    # 当前时间，用于插入 created_at / updated_at 或更新 updated_at
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 先按顺序判断记录是否存在
    record_id = get_record_id_by_priority(cursor, merged_dict)
    if record_id:
        # 如果记录存在，禁止更新
        logger.error(f"已有记录，不执行更新。IMDB: {merged_dict['imdb']} ID: {record_id}")
        return
    if record_id:
        # 如果记录存在，需要先查询数据库里这条记录的 resolution、bitrate、size 做比较
        cursor.execute(SEARCH_SQL, (record_id,))
        row = cursor.fetchone()
        if row:
            old_resolution_str, old_bitrate_str, old_size, old_tmdb, old_douban, old_imdb = row
            old_resolution_val = parse_resolution(old_resolution_str)
            old_bitrate_val = parse_bitrate(old_bitrate_str)

            new_resolution_val = parse_resolution(merged_dict['resolution'])
            new_bitrate_val = parse_bitrate(merged_dict['bitrate'])
            new_size = merged_dict['size']  # size 是整数，可直接比较

            # 如果 id 不匹配，不允许更新
            if old_tmdb and old_tmdb != merged_dict['tmdb']:
                logger.error(f"TMDB 记录不匹配，禁止更新。新 TMDB: {merged_dict['tmdb']}，旧 TMDB: {old_tmdb}，数据库 ID: {record_id}")
                return
            elif old_imdb and old_imdb != merged_dict['imdb']:
                logger.error(f"IMDB 记录不匹配，禁止更新。新 IMDB: {merged_dict['imdb']}，旧 IMDB: {old_imdb}，数据库 ID: {record_id}")
                return
            elif old_douban and old_douban != merged_dict['douban']:
                logger.error(f"DOUBAN 记录不匹配，禁止更新。新 DOUBAN: {merged_dict['douban']}，旧 DOUBAN: {old_douban}，数据库 ID: {record_id}")
                return

            # 如果新数据都大于等于旧数据才更新
            if (new_resolution_val >= old_resolution_val
                    and new_bitrate_val >= old_bitrate_val
                    and new_size >= old_size):
                update_data = data_values + (current_time, record_id)
                cursor.execute(UPDATE_SQL, update_data)
                conn.commit()
                logger.info(f"数据已更新！IMDB: {merged_dict['imdb']} ID: {record_id}")
            else:
                logger.error(f"已有记录更优，不执行更新。IMDB: {merged_dict['imdb']} ID: {record_id}")
        else:
            logger.error(f"未能查询到指定 ID 记录，跳过更新。ID: {record_id}")
    else:
        # 如果记录不存在，执行 INSERT 操作
        insert_data = data_values + (current_time, current_time)
        cursor.execute(INSERT_SQL, insert_data)
        conn.commit()
        if merged_dict['imdb']:
            logger.info(f"已插入数据库！IMDB: {merged_dict['imdb']}")
        elif merged_dict['tmdb']:
            logger.info(f"已插入数据库！TMDB: {merged_dict['tmdb']}")
        elif merged_dict['douban']:
            logger.info(f"已插入数据库！DOUBAN: {merged_dict['douban']}")
        else:
            logger.info(f"已插入数据库！ID: {cursor.lastrowid}")

    # 关闭连接
    cursor.close()
    conn.close()


def get_record_id_by_priority(cursor, merged_dict: dict) -> Any:
    """
    按 imdb -> tmdb -> douban -> 导演+标题 的顺序去数据库查找记录

    :param cursor: 数据库会话
    :param merged_dict: 完整电影信息字典
    :return: 找到则返回对应的 id，否则返回 None
    """
    # 先用 imdb 进行查找
    imdb_val = merged_dict.get('imdb')
    if imdb_val:
        select_sql = "SELECT id FROM movies WHERE imdb = %s"
        cursor.execute(select_sql, (imdb_val,))
        result = cursor.fetchone()
        if result:
            return result[0]

    # 接着用 tmdb 进行查找
    tmdb_val = merged_dict.get('tmdb')
    if tmdb_val:
        select_sql = "SELECT id FROM movies WHERE tmdb = %s"
        cursor.execute(select_sql, (tmdb_val,))
        result = cursor.fetchone()
        if result:
            return result[0]

    # 然后用 douban 进行查找，几乎没有
    douban_val = merged_dict.get('douban')
    if douban_val:
        select_sql = "SELECT id FROM movies WHERE douban = %s"
        cursor.execute(select_sql, (douban_val,))
        result = cursor.fetchone()
        if result:
            return result[0]

    # 最后用导演、标题和年份进行查找
    director_val = merged_dict.get('director')
    original_title_val = merged_dict.get('original_title')
    year_val = merged_dict.get('year')
    if director_val and original_title_val:
        select_sql = "SELECT id FROM movies WHERE director = %s AND original_title = %s AND year = %s"
        cursor.execute(select_sql, (director_val, original_title_val, year_val))
        result = cursor.fetchone()
        if result:
            return result[0]

    return None


def parse_resolution(res_str: str) -> int:
    """
    将分辨率转为实际相乘的数值

    :param res_str: 类似于 1920x800
    :return: 如果解析失败返回 0
    """
    try:
        w, h = res_str.lower().split('x')
        return int(w) * int(h)
    except Exception:
        return 0


def parse_bitrate(bitrate_str: str) -> int:
    """
    将比特率字符串转换为整数

    :param bitrate_str: 类似于 2249kbps
    :return: 如果解析失败返回 0
    """
    try:
        bitrate_str = bitrate_str.lower().replace('kbps', '').strip()
        return int(bitrate_str)
    except Exception:
        return 0
