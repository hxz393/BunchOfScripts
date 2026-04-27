"""
读取 ``movie_info.json5`` 并维护 ``movies`` / ``wanted`` 相关数据。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393
"""

import json
import logging
import os.path
from datetime import datetime
from typing import Any, Optional

import mysql.connector

from my_module import read_json_to_dict
from sort_movie_ops import parse_movie_id

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict('config/sort_movie.json')  # 配置文件

MYSQL_HOST = CONFIG['mysql_host']  # mysql 主机地址
MYSQL_USER = CONFIG['mysql_user']  # mysql 用户名
MYSQL_PASS = CONFIG['mysql_pass']  # mysql 密码
MYSQL_DB_MOVIE = CONFIG['mysql_db_movie']  # mysql 数据库
MYSQL_DB_IMDB = CONFIG['mysql_db_imdb']  # mysql 数据库

# movies 插入字段统一在这里维护，后续增删字段时优先改这份清单。
MOVIE_INSERT_FIELDS = [
    "director",
    "year",
    "original_title",
    "chinese_title",
    "genres",
    "country",
    "language",
    "runtime",
    "titles",
    "directors",
    "tmdb",
    "douban",
    "imdb",
    "source",
    "quality",
    "resolution",
    "codec",
    "bitrate",
    "duration",
    "size",
    "release_group",
    "filename",
    "version",
    "publisher",
    "pubdate",
    "dvhdr",
    "audio",
    "subtitle",
    "dl_link",
    "comment",
]
# 这些字段在库里按 JSON 字符串保存，和普通标量字段分开处理。
MOVIE_JSON_FIELDS = {
    "genres",
    "country",
    "language",
    "titles",
    "directors",
}
MOVIE_TIMESTAMP_FIELDS = ["created_at", "updated_at"]
MOVIES_INSERT_COLUMNS = MOVIE_INSERT_FIELDS + MOVIE_TIMESTAMP_FIELDS
# INSERT 列名和占位符数量都由字段清单派生，避免 SQL 列顺序和数据 tuple 手工错位。
# noinspection SqlInsertValues
MOVIES_INSERT_SQL = """
            INSERT INTO movies (
                {columns}
            ) VALUES (
                {placeholders}
            )
        """.format(
    columns=", ".join(MOVIES_INSERT_COLUMNS),
    placeholders=", ".join(["%s"] * len(MOVIES_INSERT_COLUMNS)),
)
WANTED_INSERT_SQL = """
        INSERT INTO wanted (director, year, imdb, tmdb, runtime, titles)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
IMDB_QUERY_SQL = """
    SELECT
        d.director_nconst AS director_id,
        n.primary_name AS director_name
    FROM imdb_datasets.title_directors d
    LEFT JOIN imdb_datasets.name_basics n
        ON n.nconst = d.director_nconst
    WHERE d.tconst = %s
    ORDER BY d.director_order
"""


def fetch_existing_values(cursor: Any, table_name: str, column_name: str, values: set) -> set:
    """
    批量查询表中已存在的字段值

    :param cursor: 数据库游标
    :param table_name: 表名
    :param column_name: 字段名
    :param values: 待查询的值集合
    :return: 数据库中已存在的值集合
    """
    if not values:
        return set()

    placeholders = ','.join(['%s'] * len(values))
    sql = f"SELECT {column_name} FROM {table_name} WHERE {column_name} IN ({placeholders})"
    cursor.execute(sql, tuple(values))
    return {row[0] for row in cursor.fetchall()}


def serialize_movie_field_value(field_name: str, value: Any) -> Any:
    """
    将电影字段值转换为数据库可写入的格式

    :param field_name: 字段名
    :param value: 原始值
    :return: 转换后的值
    """
    if field_name in MOVIE_JSON_FIELDS:
        return json.dumps(value, ensure_ascii=False)
    return value


def build_movie_insert_data(merged_dict: dict, current_time: str) -> tuple:
    """
    基于字段清单构造 movies 表的插入数据

    :param merged_dict: 完整电影信息字典
    :param current_time: 当前时间
    :return: 与 MOVIES_INSERT_COLUMNS 对齐的插入数据
    """
    missing_fields = [field_name for field_name in MOVIE_INSERT_FIELDS if field_name not in merged_dict]
    if missing_fields:
        raise KeyError(f"movie_info.json5 缺少必要字段: {', '.join(missing_fields)}")

    # 按字段清单顺序统一取值，这样以后增删字段只需要维护字段表本身。
    data_values = [
        serialize_movie_field_value(field_name, merged_dict[field_name])
        for field_name in MOVIE_INSERT_FIELDS
    ]
    data_values.extend([current_time, current_time])
    return tuple(data_values)


def insert_movie_wanted(wanted_list: list) -> None:
    """
    将缺失影片批量写入 wanted 表。

    写入前会先按 tmdb 去重：
    1. 跳过 wanted 表里已存在的记录。
    2. 跳过 movies 表里已经入库的记录。
    3. 跳过本次 wanted_list 内部重复的 tmdb。

    :param wanted_list: 没有记录的电影列表
    :return: 无
    """
    if not wanted_list:
        return

    conn = None
    cursor = None
    try:
        conn = create_conn()
        cursor = conn.cursor()

        # 先批量查出 wanted/movies 中已存在的 tmdb，避免逐条 N+1 查询。
        tmdb_values = {record.get('tmdb') for record in wanted_list if record.get('tmdb')}
        existing_tmdb_values = fetch_existing_tmdb_in_movies_and_wanted(cursor, tmdb_values)

        data = []
        pending_tmdb_values = set()
        for record in wanted_list:
            tmdb = record.get('tmdb')
            if tmdb and (tmdb in existing_tmdb_values or tmdb in pending_tmdb_values):
                continue

            if tmdb:
                pending_tmdb_values.add(tmdb)

            data.append((
                record.get('director'),
                record.get('year') if record.get('year') else 0,
                record.get('imdb') if record.get('imdb') else None,
                tmdb,
                record.get('runtime'),
                json.dumps(record.get('titles'), ensure_ascii=False),
            ))

        if data:
            cursor.executemany(WANTED_INSERT_SQL, data)
            conn.commit()
            logger.info(f"wanted 库 {cursor.rowcount} 条记录插入成功")
    except mysql.connector.Error as err:
        if conn:
            conn.rollback()
        logger.error(f"写入 wanted 表失败：{err}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def insert_movie_record_to_mysql(path: str) -> None:
    """
    读取目录下的 ``movie_info.json5``，并在 movies 表中插入一条记录。

    当前逻辑只负责“查重后插入”，不再执行旧的更新分支。
    如果本条电影带 tmdb，则会在同一事务里顺带清理 wanted 表中的待办记录。

    :param path: 电影目录
    :return: 无
    """
    logger.info("-" * 25 + "步骤：写入数据库" + "-" * 25)
    merged_dict = read_json_to_dict(os.path.join(path, "movie_info.json5"))
    if not merged_dict:
        logger.error("无法读取 JSON 文件")
        return

    conn = None
    cursor = None
    try:
        conn = create_conn()
        cursor = conn.cursor()

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        record_id = get_record_id_by_priority(cursor, merged_dict)
        if record_id:
            logger.error(f"已有记录，不执行插入。IMDB: {merged_dict['imdb']} ID: {record_id}")
            return

        insert_data = build_movie_insert_data(merged_dict, current_time)
        cursor.execute(MOVIES_INSERT_SQL, insert_data)
        inserted_rowid = cursor.lastrowid

        if merged_dict['tmdb']:
            # 保持和 movies 插入同一事务，避免一边插入成功、一边 wanted 清理失败。
            cursor.execute("DELETE FROM wanted WHERE tmdb = %s", (merged_dict['tmdb'],))

        conn.commit()

        if merged_dict['imdb']:
            logger.info(f"已插入数据库！IMDB: {merged_dict['imdb']}")
        elif merged_dict['tmdb']:
            logger.info(f"已插入数据库！TMDB: {merged_dict['tmdb']}")
        elif merged_dict['douban']:
            logger.info(f"已插入数据库！DOUBAN: {merged_dict['douban']}")
        else:
            logger.info(f"已插入数据库！ID: {inserted_rowid}")
    except KeyError as err:
        logger.error(f"电影信息字段缺失：{err.args[0]}")
        raise
    except mysql.connector.Error as err:
        if conn:
            conn.rollback()
        logger.error(f"写入 movies 表失败：{err}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def create_conn(database: str = MYSQL_DB_MOVIE) -> Any:
    """
    创建指定数据库连接；默认连接 movies 业务库。

    :param database: 数据库名
    :return: 返回数据库连接
    """
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=database
    )
    return conn


def get_batch_by_imdb(conn: Any, table_name: str, imdb_ids: set) -> list[dict]:
    """
    按一批 IMDb 编号批量查询指定表记录。

    :param conn: 数据库会话
    :param table_name: 表名，仅允许 ``wanted`` 或 ``movies``
    :param imdb_ids: imdb_id 集合
    :return: 命中的记录列表；空输入时返回空列表
    """
    if not imdb_ids:
        return []

    if table_name not in {"wanted", "movies"}:
        raise ValueError(f"不支持的表名: {table_name}")

    # 构造 SQL IN 查询
    placeholders = ','.join(['%s'] * len(imdb_ids))  # 例如: "%s,%s,%s"
    sql = f"SELECT * FROM {table_name} WHERE imdb IN ({placeholders})"

    with conn.cursor(dictionary=True) as cursor:
        cursor.execute(sql, tuple(imdb_ids))
        return cursor.fetchall()


def build_parsed_movie_ids(ids: list) -> list[tuple[str, Optional[tuple[str, str]]]]:
    """
    把原始电影 id 列表解析成 ``(原始值, 规范化结果)`` 结构。

    :param ids: 原始电影 id 列表
    :return: 解析结果列表；未知前缀会保留为 ``(原始值, None)``
    """
    return [(movie_id, parse_movie_id(movie_id)) for movie_id in ids]


def fetch_existing_movie_external_ids(cursor: Any, parsed_ids: list[tuple[str, Optional[tuple[str, str]]]]) -> dict[str, set]:
    """
    按 ``imdb / tmdb / douban`` 三种外部编号，批量查询 movies 表中已存在的值。

    这个 helper 只负责收口三类外部 id 的批量查询，供单条插入查重和批量缺失检查共用。

    :param cursor: 数据库游标
    :param parsed_ids: ``build_parsed_movie_ids()`` 产出的解析结果
    :return: 已存在的外部 id 集合，按字段名分组
    """
    grouped_ids = {
        "imdb": set(),
        "tmdb": set(),
        "douban": set(),
    }

    for _, parsed in parsed_ids:
        if parsed:
            grouped_ids[parsed[0]].add(parsed[1])

    return {
        "imdb": fetch_existing_values(cursor, "movies", "imdb", grouped_ids["imdb"]),
        "tmdb": fetch_existing_values(cursor, "movies", "tmdb", grouped_ids["tmdb"]),
        "douban": fetch_existing_values(cursor, "movies", "douban", grouped_ids["douban"]),
    }


def fetch_existing_tmdb_in_movies_and_wanted(cursor: Any, tmdb_values: set) -> set:
    """
    批量查询 movies / wanted 两张表里已存在的 tmdb 编号。

    这个 helper 专门服务于 wanted 去重和导演作品抓取前的跳过逻辑。

    :param cursor: 数据库游标
    :param tmdb_values: 待检查的 tmdb 编号集合
    :return: 已存在于 movies 或 wanted 的 tmdb 编号集合
    """
    existing_tmdb_values = fetch_existing_values(cursor, "movies", "tmdb", tmdb_values)
    existing_tmdb_values.update(fetch_existing_values(cursor, "wanted", "tmdb", tmdb_values))
    return existing_tmdb_values


def check_movie_ids(ids: list) -> list:
    """批量检查电影 id 是否已存在于 movies 表中。

    支持三种输入前缀：
    - ``tt``: IMDb 编号
    - ``tmdb``: TMDb 编号
    - ``db``: 豆瓣编号

    返回值保持原输入顺序，只保留未命中的 id。

    :param ids: 电影 id 列表
    :return: 没查询到的电影 id 列表
    """
    conn = None
    cursor = None
    try:
        conn = create_conn()
        cursor = conn.cursor()

        parsed_ids = build_parsed_movie_ids(ids)
        existing_ids = fetch_existing_movie_external_ids(cursor, parsed_ids)

        return [
            movie_id
            for movie_id, parsed in parsed_ids
            if not parsed or parsed[1] not in existing_ids[parsed[0]]
        ]
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def query_imdb_local_director(movie_id: str) -> Optional[list[dict]]:
    """
    根据 IMDb 影片编号查询本地镜像库里的导演列表。

    :param movie_id: imdb 编号
    :return: 搜索结果，成功则返回导演字典列表，形如 [{"director_id": "nmxxxxxxx", "director_name": "导演名"}, ...]
    """
    conn = None
    cursor = None
    try:
        # 建立数据库连接
        conn = create_conn(MYSQL_DB_IMDB)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(IMDB_QUERY_SQL, (movie_id,))
        directors = cursor.fetchall()
        return directors
    except mysql.connector.Error as e:
        logger.error(f"IMDb 本地库查询失败！{movie_id} {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def get_record_id_by_priority(cursor, merged_dict: dict) -> Any:
    """
    按 ``imdb -> tmdb -> douban`` 的顺序查找 movies 表中是否已有对应记录。

    单条入库路径直接按优先级回查数据库主键，避免先做存在性判断再多一次往返。

    :param cursor: 数据库游标
    :param merged_dict: 完整电影信息字典
    :return: 找到则返回对应的 id，否则返回 None
    """
    for field_name in ("imdb", "tmdb", "douban"):
        field_value = merged_dict.get(field_name)
        if not field_value:
            continue

        select_sql = f"SELECT id FROM movies WHERE {field_name} = %s"
        cursor.execute(select_sql, (field_value,))
        result = cursor.fetchone()
        if result:
            return result[0]

    return None


def remove_existing_tmdb_ids(tmdb_set: set) -> set:
    """
    过滤掉在 movies / wanted 表中已经存在的 tmdb 编号。

    注意：这里不会删除数据库记录，只会从传入集合里移除已经存在的 tmdb。

    :param tmdb_set: tmdb 编号集合
    :return: 过滤后的 tmdb 编号集合
    """
    if not tmdb_set:
        return tmdb_set

    conn = None
    cursor = None
    try:
        conn = create_conn()
        cursor = conn.cursor()
        tmdb_set.difference_update(fetch_existing_tmdb_in_movies_and_wanted(cursor, tmdb_set))
        return tmdb_set
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def delete_records(id_list: list, id_type: str, table_name: str) -> None:
    """
    按指定字段值批量删除记录。

    这是内部固定用途辅助函数，调用方应只传入可信的表名和字段名。

    :param id_list: 编号列表
    :param id_type: 编号类型
    :param table_name: 表名
    :return: 无
    """
    # 过滤掉空元素
    id_list = [type_id for type_id in id_list if type_id]
    if not id_list:
        logger.info(f'{table_name} 表无有效的删除编号，跳过操作')
        return

    conn = None
    cursor = None
    try:
        conn = create_conn()
        cursor = conn.cursor()
        # 参数化查询，防止SQL注入
        sql = f"DELETE FROM {table_name} WHERE {id_type} = %s"
        # 批量执行删除
        cursor.executemany(sql, [(type_id,) for type_id in id_list])
        conn.commit()
        counts = cursor.rowcount
        if counts:
            logger.info(f'成功删除 {table_name} 表中 {counts} 条数据')
    except mysql.connector.Error as err:
        if conn:
            conn.rollback()
        logger.error(f'操作异常：{err}')
        raise
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
