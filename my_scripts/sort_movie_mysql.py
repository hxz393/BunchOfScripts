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
        # 如果记录存在，执行 UPDATE 操作
        update_sql = """
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
        update_data = data_values + (current_time, record_id)
        cursor.execute(update_sql, update_data)
        conn.commit()
        logger.info(f"数据已更新！IMDB: {merged_dict['imdb']}")
    else:
        # 如果记录不存在，执行 INSERT 操作
        insert_sql = """
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
        insert_data = data_values + (current_time, current_time)
        cursor.execute(insert_sql, insert_data)
        conn.commit()
        print(f"已插入数据库！IMDB: {merged_dict['imdb']}")

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
    imdb_val = merged_dict.get('imdb')
    tmdb_val = merged_dict.get('tmdb')
    douban_val = merged_dict.get('douban')
    director_val = merged_dict.get('director')
    original_title_val = merged_dict.get('original_title')

    # 先用 imdb 进行查找，排除默认的 'tt0000000' 这种无效值
    if imdb_val and imdb_val != 'tt0000000':
        select_sql = "SELECT id FROM movies WHERE imdb = %s"
        cursor.execute(select_sql, (imdb_val,))
        result = cursor.fetchone()
        if result:
            return result[0]

    # 接着用 tmdb 进行查找，排除可能的空值或 '0'
    if tmdb_val and tmdb_val not in ['0', '']:
        select_sql = "SELECT id FROM movies WHERE tmdb = %s"
        cursor.execute(select_sql, (tmdb_val,))
        result = cursor.fetchone()
        if result:
            return result[0]

    # 再用 douban 进行查找
    if douban_val and douban_val not in ['0', '']:
        select_sql = "SELECT id FROM movies WHERE douban = %s"
        cursor.execute(select_sql, (douban_val,))
        result = cursor.fetchone()
        if result:
            return result[0]

    # 最后用导演和标题进行查找
    if director_val and original_title_val:
        select_sql = "SELECT id FROM movies WHERE director = %s AND original_title = %s"
        cursor.execute(select_sql, (director_val, original_title_val))
        result = cursor.fetchone()
        if result:
            return result[0]

    return None
