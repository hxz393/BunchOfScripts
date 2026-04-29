"""
新发布种子整理辅助功能。

这些功能目前是待大修的旧流程，暂时只从 ``sort_movie_ops`` 拆出，逻辑不做调整。
"""
import logging
import os
import shutil
from pathlib import Path

from my_module import read_json_to_dict, get_file_paths, get_folder_paths
from sort_movie_ops import extract_imdb_id

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict('config/sort_movie_ops.json')  # 配置文件

MIRROR_PATH = CONFIG['mirror_path']  # 镜像文件夹路径
RU_PATH = CONFIG['ru_path']  # ru 种子路径
YTS_PATH = CONFIG['yts_path']  # yts 种子路径
DHD_PATH = CONFIG['dhd_path']  # dhd 种子路径
TTG_PATH = CONFIG['ttg_path']  # ttg 种子路径
SK_PATH = CONFIG['sk_path']  # ttg 种子路径
RARE_PATH = CONFIG['rare_path']  # rare 文件路径

BD_SOURCE = ['BDRemux', 'BluRay', 'BDRip']


def sort_new_torrents_by_director(target_path: str) -> None:
    """
    整理种子目录，将已整理过的导演电影种子提取出来

    :param target_path: 移动目标目录
    :return: 无
    """
    # 获取列表
    keywords = sorted(
        [os.path.basename(key) for key in get_folder_paths(MIRROR_PATH)],
        key=str.casefold
    )
    # 遍历 YTS 目录，找到关键词就跳出，保证列表优先级
    for yts_path in get_folder_paths(YTS_PATH):
        basename = os.path.basename(yts_path).lower()
        matched_keyword = None

        for kw in keywords:
            if kw.lower() in basename:
                matched_keyword = kw
                break
        if matched_keyword:
            logger.info(f"关键字 '{matched_keyword}' 匹配到路径: {yts_path}")
            shutil.move(yts_path, target_path)

    # 遍历 RU 目录
    for ru_path in get_file_paths(RU_PATH):
        basename = os.path.basename(ru_path).lower()
        matched_keyword = None

        for kw in keywords:
            if kw.lower() in basename:
                matched_keyword = kw
                break
        if matched_keyword:
            target_path_root = os.path.join(target_path, matched_keyword)
            Path(target_path_root).mkdir(parents=True, exist_ok=True)
            target_path_ru = os.path.join(target_path_root, os.path.basename(ru_path))
            logger.info(f"关键字 '{matched_keyword}' 匹配到路径: {ru_path}")
            shutil.move(ru_path, target_path_ru)


def sort_new_torrents_by_mysql(target_path: str) -> None:
    """
    搜索数据库，将已整理过的导演电影种子提取出来

    :param target_path: 移动目标目录
    :return: 无
    """

    def delete_torrent(delete_path: str):
        """辅助函数，删除种子"""
        logger.info(f"{imdb_id} 已删除（{quality} {source}）：{delete_path}")
        os.remove(delete_path)

    def move_torrent(source_path: str, director_name: str):
        """辅助函数，移动种子"""
        logger.info(f"{imdb_id} 已匹配（{quality} {source}）：{file_path}")
        target_path_root = os.path.join(target_path, director_name)
        Path(target_path_root).mkdir(parents=True, exist_ok=True)
        target_path_file = os.path.join(target_path_root, os.path.basename(source_path))
        shutil.move(source_path, target_path_file)

    # 建立数据库连接
    from sort_movie_mysql import create_conn, get_batch_by_imdb
    conn = create_conn()
    # 获取种子列表
    file_path_list = get_file_paths(DHD_PATH) + get_file_paths(TTG_PATH) + get_file_paths(SK_PATH) + get_file_paths(RARE_PATH)
    # 扫描所有文件得到 imdb_id_set
    imdb_id_set = {imdb_id for f in file_path_list if (imdb_id := extract_imdb_id(f))}

    # 批量取出数据
    wanted_rows = get_batch_by_imdb(conn, "wanted", imdb_id_set)
    movie_rows = get_batch_by_imdb(conn, "movies", imdb_id_set)

    wanted_map = {row['imdb']: row for row in wanted_rows}
    movie_map = {row['imdb']: row for row in movie_rows}
    for file_path in file_path_list:
        # 获取 IMDB 编号，不存在则跳过
        imdb_id = extract_imdb_id(file_path)
        if not imdb_id:
            continue

        # 去缺少库搜索，找到了直接移动
        if imdb_id in wanted_map:
            move_torrent(file_path, wanted_map[imdb_id]['director'])
            continue

        # 去收集库搜索，没找到直接跳过
        if imdb_id not in movie_map:
            continue

        # 复杂的筛选逻辑
        movie_info = movie_map[imdb_id]
        director = movie_info['director']
        quality = movie_info['quality']
        source = movie_info['source']
        # 文件维度，两个指标，来源和质量
        if '1080p' in file_path.lower():
            torrent_quality = '1080p'
        elif '2160p' in file_path.lower():
            torrent_quality = '2160p'
        else:
            torrent_quality = 'other'

        if r'B:\0.整理\BT\dhd' in file_path:
            torrent_source = 'dhd'
        elif r'B:\0.整理\BT\ttg' in file_path:
            torrent_source = 'ttg'
        elif r'B:\0.整理\BT\sk' in file_path:
            torrent_source = 'sk'
        elif r'B:\0.整理\BT\rare' in file_path:
            torrent_source = 'rare'
        else:
            logger.error('程序错误')
            return

        # 2160p 蓝光，直接删除所有类型种子
        if quality == '2160p' and source in BD_SOURCE:
            delete_torrent(file_path)
            continue

        # 1080p 蓝光，直接删除 1080p 种子
        if quality == '1080p' and source in BD_SOURCE and torrent_quality == '1080p':
            delete_torrent(file_path)
            continue

        # 1080p 以上画质，直接删除 rare 目录种子
        if quality in ['1080p', '2160p'] and torrent_source == 'rare':
            delete_torrent(file_path)
            continue

        # 其他情况，保留种子
        if quality:
            move_torrent(file_path, director)
            continue
