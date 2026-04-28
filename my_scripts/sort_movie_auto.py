"""
自动整理电影目录并补充影片元数据。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import logging
import os
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

from my_module import sanitize_filename, write_dict_to_json
from sort_movie_mysql import insert_movie_record_to_mysql, query_imdb_title_metadata
from sort_movie_ops import (
    build_movie_folder_name,
    check_movie,
    create_aka_movie,
    extract_imdb_id,
    fix_douban_name,
    get_movie_id,
    get_video_info,
    merged_dict,
    move_all_files_to_root,
    scan_ids,
)
from sort_movie_request import (
    get_douban_response,
    get_douban_search_details,
    get_tmdb_movie_cover,
    get_tmdb_movie_details,
    get_tmdb_search_response,
)

logger = logging.getLogger(__name__)
# TMDB 的真实低位编号很多（例如两位、三位都可能出现），
# 这里只要求至少 2 位数字，避免把 ``tmdb1`` 之类明显噪声当成有效编号。
TMDB_FOLDER_ID_PATTERN = re.compile(r"(?<![A-Za-z0-9])(tmdb\d{2,}(?:tv)?)(?![A-Za-z0-9])", re.IGNORECASE)
# 豆瓣编号在目录名里出现的概率远低于普通 ``db123`` 之类噪声，
# 这里要求至少 6 位数字，降低误匹配，同时保留现有手工样本位数。
DOUBAN_FOLDER_ID_PATTERN = re.compile(r"(?<![A-Za-z0-9])(db\d{6,})(?![A-Za-z0-9])", re.IGNORECASE)
FAILED_MOVIE_ROOT = r"A:\0d.检验转码"
PrepareFolderError = tuple[str, str]


def sort_movie_auto(path: str) -> None:
    """
    批量处理根目录下的电影子目录。

    单个目录如果在入口校验阶段失败，会被移到检验目录后继续处理后续目录。

    :param path: 包含多个电影子目录的根目录
    :return: 无
    """
    folders = [os.path.join(path, item) for item in os.listdir(path) if os.path.isdir(os.path.join(path, item))]
    if not folders:
        logger.error(f"目录下没有子文件夹 {path}")
        return

    for folder in folders:
        logger.info(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 开始处理：{folder}")
        logger.info("-" * 25 + "步骤：搜索电影" + "-" * 25)
        result = prepare_movie_folder_markers(folder)
        if result:
            _error_code, error_message = result
            logger.error(error_message)
            move_result = move_failed_movie_folder(folder, path)
            if move_result:
                logger.error(move_result)
            logger.warning("=" * 255)
            continue
        # 先将子目录中的文件提升到当前电影目录根部
        move_all_files_to_root(folder)
        time.sleep(0.1)
        logger.info("-" * 25 + "步骤：抓取电影信息" + "-" * 25)
        sort_movie(folder)
        time.sleep(0.1)
        logger.warning("=" * 255)
        time.sleep(0.1)


def build_prepare_folder_error(code: str, message: str) -> PrepareFolderError:
    """
    构造入口预处理阶段的轻量错误结果。

    :param code: 稳定的错误代码，供上层或测试按类型判断
    :param message: 面向日志和人工排查的错误文本
    :return: ``(code, message)``
    """
    return code, message


def apply_auto_resolved_marker(path: str, existing_ids: dict[str, Optional[str]], id_type: str, id_value: str, label: str) -> Optional[PrepareFolderError]:
    """
    应用 IMDb 自动补出的同类型编号，并检查是否与现有空文件冲突。

    :param path: 单个电影目录路径
    :param existing_ids: 当前目录已有的编号空文件结果
    :param id_type: 编号类型，只支持 ``tmdb`` 或 ``douban``
    :param id_value: 自动补出的编号值
    :param label: 日志和错误提示中使用的大写站点名称
    :return: 冲突时返回 ``(code, message)``，否则返回 ``None``
    """
    if not id_value:
        return None

    if existing_ids[id_type] and existing_ids[id_type] != id_value:
        return build_prepare_folder_error(f"auto_{id_type}_marker_conflict", f"IMDb 自动补出的 {label} 编号与空文件不一致 {path}")

    touch_id_marker(path, id_value, id_type)
    return None


def prepare_movie_folder_markers(path: str) -> Optional[PrepareFolderError]:
    """
    为单个电影目录补齐编号空文件，并决定是否允许进入抓取步骤。

    目录名是入口阶段唯一可信的编号来源：
    - 目录名有 ``tt...`` 时，会先验证该编号在本地 IMDb 镜像中仍然有效，再创建 ``.imdb`` 并自动补查 ``.tmdb/.douban``
    - 目录名只有 ``tmdb...`` / ``db...`` 时，要求目录里已有人工确认的对应空文件

    :param path: 单个电影目录路径
    :return: 失败时返回 ``(code, message)``，成功时返回 ``None``
    """
    folder_name_ids = get_folder_name_ids(path)
    existing_ids, error = get_existing_id_files(path)
    if error:
        return build_prepare_folder_error("duplicate_marker_files", error)

    error = validate_folder_name_ids(path, folder_name_ids, existing_ids)
    if error:
        return build_prepare_folder_error("folder_name_marker_conflict", error)

    imdb_id = folder_name_ids["imdb"]
    if not imdb_id:
        if folder_name_ids["tmdb"] or folder_name_ids["douban"]:
            if not existing_ids["tmdb"] and not existing_ids["douban"]:
                return build_prepare_folder_error("missing_verified_manual_marker", f"目录名带有 TMDB/DOUBAN 编号，但缺少已确认的 .tmdb/.douban 空文件 {path}")
            return None
        return build_prepare_folder_error("missing_supported_folder_id", f"目录名缺少受支持的电影编号 {path}")

    if not query_imdb_title_metadata(imdb_id):
        remove_id_marker(path, imdb_id, "imdb")
        return build_prepare_folder_error("obsolete_imdb_id", f"IMDb 本地库没有找到影片信息，tt 编号过旧需要更新 {imdb_id} {path}")

    touch_id_marker(path, imdb_id, "imdb")

    # 查询 TMDB，补充编号空文件
    r = get_tmdb_id(imdb_id)
    if r["result"]:
        logger.error(r["result"])
    tmdb_id = r["tmdb_id"]
    error = apply_auto_resolved_marker(path, existing_ids, "tmdb", tmdb_id, "TMDB")
    if error:
        return error

    # 查询 Douban，补充编号空文件
    r = get_douban_id(imdb_id)
    if r["result"]:
        logger.error(r["result"])
    douban_id = r["douban_id"]
    error = apply_auto_resolved_marker(path, existing_ids, "douban", douban_id, "DOUBAN")
    if error:
        return error

    return None


def build_empty_movie_info() -> dict:
    """
    创建统一的影片元数据容器，供各站点逐步补全。

    :return: 初始化后的电影信息字典
    """
    return {
        "director": "",
        "year": 0,
        "original_title": "",
        "chinese_title": "",
        "genres": [],
        "country": [],
        "language": [],
        "runtime": 0,
        "poster_path": "",
        "titles": [],
        "directors": [],
        "version": "",
        "publisher": "",
        "pubdate": "",
        "dvhdr": "",
        "audio": "未知",
        "subtitle": "未知",
        "comment": None,
    }


def fill_movie_info(movie_ids: dict, movie_info: dict, tv: bool) -> None:
    """
    按可用编号依次补充 TMDB、IMDb 和 Douban 元数据。

    :param movie_ids: 当前目录扫描到的编号字典
    :param movie_info: 统一的电影信息字典，会在原对象上更新
    :param tv: 是否按电视剧条目处理
    :return: 无
    """
    tmdb_id = movie_ids.get("tmdb")
    if tmdb_id:
        normalized_tmdb_id = tmdb_id[:-2] if tmdb_id.endswith("tv") else tmdb_id
        get_tmdb_movie_info(normalized_tmdb_id, movie_info, tv)
    else:
        logger.warning("没有 TMDB 编号。")

    imdb_id = movie_ids.get("imdb")
    if imdb_id:
        get_imdb_movie_info(imdb_id, movie_info)
    else:
        logger.warning("没有 IMDB 编号。")

    douban_id = movie_ids.get("douban")
    if douban_id:
        get_douban_movie_info(douban_id, movie_info)
    else:
        logger.warning("没有 DOUBAN 编号。")


def get_created_file_names(path: str, original_file_names: set[str]) -> set[str]:
    """
    计算当前目录里相对整理前新出现的文件名集合。

    :param path: 当前电影目录
    :param original_file_names: 整理前已有的文件名集合
    :return: 新增文件名集合
    """
    return {file.name for file in Path(path).iterdir() if file.is_file()} - original_file_names


def rollback_sort_movie_state(path: str, current_path: str, renamed: bool, created_file_names: set[str], movie_info_backup: Optional[bytes]) -> None:
    """
    将整理失败后的目录状态回滚到整理前。

    :param path: 整理前原始目录路径
    :param current_path: 当前目录路径，可能已经重命名
    :param renamed: 是否执行过目录重命名
    :param created_file_names: 本次整理新创建的文件名集合
    :param movie_info_backup: 整理前 ``movie_info.json5`` 的原始字节；不存在时为 ``None``
    :return: 无
    """
    rollback_path = Path(current_path)
    if rollback_path.exists():
        for file_name in created_file_names:
            file_path = rollback_path / file_name
            if file_path.exists():
                file_path.unlink()
        if movie_info_backup is not None:
            (rollback_path / "movie_info.json5").write_bytes(movie_info_backup)
    if renamed and os.path.exists(current_path) and not os.path.exists(path):
        os.rename(current_path, path)


def apply_sort_movie_transaction(path: str, new_path: str, movie_dict: dict) -> Optional[str]:
    """
    执行目录重命名、落盘、校验和入库，并在失败时回滚。

    :param path: 整理前原始目录路径
    :param new_path: 目标目录路径
    :param movie_dict: 完整电影信息字典
    :return: 成功时返回最终目录路径，失败时返回 ``None``
    """
    same_target = os.path.normcase(os.path.abspath(path)) == os.path.normcase(os.path.abspath(new_path))
    if not same_target and os.path.exists(new_path):
        logger.error(f"目标目录已存在，停止整理：{new_path}")
        return None

    current_path = path
    renamed = False
    original_file_names = {file.name for file in Path(path).iterdir() if file.is_file()}
    created_file_names: set[str] = set()
    movie_info_path = Path(path) / "movie_info.json5"
    movie_info_backup = movie_info_path.read_bytes() if movie_info_path.exists() else None

    try:
        if not same_target:
            os.rename(path, new_path)
            current_path = new_path
            renamed = True

        logger.info(f"抓取结果：{movie_dict}")

        image_path = os.path.join(current_path, f"{get_movie_id(movie_dict)}.jpg")
        if not os.path.exists(image_path):
            get_tmdb_movie_cover(movie_dict["poster_path"], image_path)
            created_file_names.update(get_created_file_names(current_path, original_file_names))

        create_aka_movie(current_path, movie_dict)
        created_file_names.update(get_created_file_names(current_path, original_file_names))

        write_dict_to_json(os.path.join(current_path, "movie_info.json5"), movie_dict)
        created_file_names.update(get_created_file_names(current_path, original_file_names))

        time.sleep(0.1)
        logger.info("-" * 25 + "步骤：检查校验信息" + "-" * 25)
        time.sleep(0.1)
        check_result = check_movie(current_path)
        if check_result:
            logger.error(check_result)
            rollback_sort_movie_state(path, current_path, renamed, created_file_names, movie_info_backup)
            return None

        insert_movie_record_to_mysql(current_path)
        time.sleep(0.1)
        logger.info(f"旧名：{path}")
        logger.info(f"新名：{current_path}")
        return current_path
    except Exception:
        logger.exception(f"整理失败，开始回滚：{path}")
        rollback_sort_movie_state(path, current_path, renamed, created_file_names, movie_info_backup)
        return None


def sort_movie(path: str, tv: bool = False) -> None:
    """
    根据目录中的编号文件抓取影片信息，并完成目录整理。

    :param path: 待整理的电影目录路径
    :param tv: 是否按电视剧条目处理；当 TMDB 编号带 ``tv`` 后缀时会自动覆盖
    :return: 无
    """
    path = path.strip()
    if not os.path.exists(path):
        logger.error("目录不存在")
        return

    # 只要没有任何 ID，就直接拒绝继续处理和入库
    movie_ids = scan_ids(path)
    if all(value is None for value in movie_ids.values()):
        logger.error("没有找到任何 ID")
        return

    # TMDB 电视剧编号以 ``tv`` 后缀标记，这里据此自动切换抓取模式
    tv = bool(movie_ids["tmdb"] and movie_ids["tmdb"].endswith("tv"))
    movie_info = build_empty_movie_info()
    fill_movie_info(movie_ids, movie_info, tv)

    # 读取本地视频文件的基础信息
    file_info = get_video_info(path)
    if not file_info:
        return

    # 合并线上元数据、编号信息和本地视频信息
    movie_dict = merged_dict(path, movie_info, movie_ids, file_info)
    # 根据整理规则生成新目录名
    new_path = os.path.join(os.path.dirname(path), sanitize_filename(build_movie_folder_name(path, movie_dict)))
    apply_sort_movie_transaction(path, new_path, movie_dict)


def get_folder_name_ids(path: str) -> dict[str, Optional[str]]:
    """
    从目录名中提取可用的电影编号。

    目录名是自动整理入口阶段唯一可信的编号来源，因此这里只解析目录名，
    不参考目录里的空文件内容。

    :param path: 电影目录路径
    :return: 包含 ``imdb`` / ``tmdb`` / ``douban`` 的结果字典
    """
    folder_name = os.path.basename(os.path.normpath(path))
    imdb_id = extract_imdb_id(folder_name)
    tmdb_match = TMDB_FOLDER_ID_PATTERN.search(folder_name)
    douban_match = DOUBAN_FOLDER_ID_PATTERN.search(folder_name)
    return {
        "imdb": imdb_id,
        "tmdb": tmdb_match.group(1)[4:].lower() if tmdb_match else None,
        "douban": douban_match.group(1)[2:] if douban_match else None,
    }


def get_existing_id_files(path: str) -> tuple[dict[str, Optional[str]], Optional[str]]:
    """
    读取目录里现有的编号空文件，并检测是否存在同类型重复文件。

    :param path: 电影目录路径
    :return: ``(编号字典, 错误信息)``
    """
    id_files: dict[str, list[str]] = {"imdb": [], "tmdb": [], "douban": []}
    ext_map = {".imdb": "imdb", ".tmdb": "tmdb", ".douban": "douban"}

    try:
        file_names = os.listdir(path)
    except FileNotFoundError:
        return {"imdb": None, "tmdb": None, "douban": None}, f"目录不存在 {path}"

    for file_name in file_names:
        name, ext = os.path.splitext(file_name)
        key = ext_map.get(ext)
        if key:
            id_files[key].append(name)

    for key, values in id_files.items():
        if len(values) > 1:
            return {"imdb": None, "tmdb": None, "douban": None}, f"目录 {path} 中 {key.upper()} 编号文件太多，请先清理。"

    return {key: values[0] if values else None for key, values in id_files.items()}, None


def validate_folder_name_ids(path: str, folder_name_ids: dict[str, Optional[str]], existing_ids: dict[str, Optional[str]]) -> Optional[str]:
    """
    检查目录名中的编号与现有空文件是否冲突。

    :param path: 电影目录路径
    :param folder_name_ids: 从目录名提取出的编号
    :param existing_ids: 目录中现有空文件携带的编号
    :return: 冲突时返回错误信息，否则返回 ``None``
    """
    labels = {"imdb": "IMDB", "tmdb": "TMDB", "douban": "DOUBAN"}
    for key, label in labels.items():
        folder_id = folder_name_ids.get(key)
        file_id = existing_ids.get(key)
        if folder_id and file_id and folder_id != file_id:
            return f"目录名中的 {label} 编号与空文件不一致 {path}"
    return None


def touch_id_marker(path: str, id_value: str, suffix: str) -> None:
    """
    在目录中创建编号空文件；若已存在则保持不变。

    :param path: 单个电影目录路径
    :param id_value: 编号值
    :param suffix: 空文件后缀，不含点
    :return: 无
    """
    Path(path, f"{id_value}.{suffix}").touch()


def remove_id_marker(path: str, id_value: str, suffix: str) -> None:
    """
    删除目录中的指定编号空文件；不存在时静默跳过。

    :param path: 单个电影目录路径
    :param id_value: 编号值
    :param suffix: 空文件后缀，不含点
    :return: 无
    """
    marker_path = Path(path, f"{id_value}.{suffix}")
    if marker_path.exists():
        marker_path.unlink()


def move_failed_movie_folder(movie_path: str, source_root: str) -> Optional[str]:
    """
    将入口校验失败的电影目录移到检验目录，并保留上层导演目录。

    :param movie_path: 当前失败的电影目录
    :param source_root: 当前批处理根目录，一般是导演目录
    :return: 失败时返回原因，成功时返回 ``None``
    """
    if not os.path.exists(movie_path):
        return f"失败目录不存在，无法移到检验目录 {movie_path}"

    director_name = os.path.basename(os.path.normpath(source_root))
    target_dir = os.path.join(FAILED_MOVIE_ROOT, director_name)
    os.makedirs(target_dir, exist_ok=True)

    target_path = os.path.join(target_dir, os.path.basename(os.path.normpath(movie_path)))
    if os.path.exists(target_path):
        return f"检验目录中已存在同名目录 {target_path}"

    shutil.move(movie_path, target_path)
    logger.warning(f"已移到检验目录：{target_path}")
    return None


def get_tmdb_id(imdb_id: str) -> dict:
    """
    根据 IMDb 编号查询 TMDB 编号。

    :param imdb_id: IMDb 电影编号
    :return: 包含错误信息和 TMDB 编号的结果字典；电视剧编号会带 ``tv`` 后缀
    """
    return_dict = {"result": "", "tmdb_id": ""}
    # 通过 IMDb 编号调用 TMDB find 接口
    search_result = get_tmdb_search_response(imdb_id)
    movie_results = search_result.get('movie_results') or []
    tv_results = search_result.get('tv_results') or []
    if not movie_results and not tv_results:
        return_dict["result"] = f"tmdb 搜索失败，没找到电影"
        return return_dict

    # 电影优先返回纯数字编号；电视剧返回带 ``tv`` 后缀的编号
    result_row = movie_results[0] if movie_results else tv_results[0]
    tmdb_id = result_row.get('id')
    if not tmdb_id:
        return_dict["result"] = f"获取 tmdb 编号失败"
        return return_dict

    return_dict["tmdb_id"] = f"{tmdb_id}tv" if tv_results and not movie_results else str(tmdb_id)
    return return_dict


def get_douban_id(imdb_id: str) -> dict:
    """
    根据 IMDb 编号查询 Douban 编号。

    :param imdb_id: IMDb 电影编号
    :return: 包含错误信息和 Douban 编号的结果字典
    """
    return_dict = {"result": "", "douban_id": ""}
    # 先取 Douban 搜索页响应
    response = get_douban_response(imdb_id, "movie_search")
    if not response:
        return_dict["result"] = "豆瓣电影搜索失败"
        return return_dict

    # 再从搜索结果中提取详情页链接
    inner_url = get_douban_search_details(response)
    if not inner_url:
        return return_dict

    match = re.search(r'/subject/(\d+)', inner_url)
    if not match:
        return_dict["result"] = "豆瓣链接里没有有效编号"
        return return_dict

    return_dict["douban_id"] = match.group(1)
    return return_dict


def get_tmdb_movie_info(movie_id: str, movie_info: dict, tv: bool) -> None:
    """
    从 TMDB 详情接口提取并合并影片元数据。

    :param movie_id: TMDB 电影或电视剧编号
    :param movie_info: 电影信息字典，会在原对象上更新
    :param tv: 是否是电视剧
    :return: 无
    """
    m = get_tmdb_movie_details(movie_id, tv)
    if not m:
        return

    # 基础字段
    movie_info["genres"] = [i['name'] for i in m['genres']] if m.get('genres') else []
    movie_info["country"] = [i for i in m['origin_country']]
    movie_info["language"] = [m['original_language']]
    movie_info["original_title"] = m['original_name' if tv else 'original_title']
    movie_info["year"] = m['first_air_date' if tv else 'release_date']
    movie_info["year"] = movie_info["year"][:4] if movie_info["year"] else None

    # 时长字段
    runtime_tmdb = 0
    if tv:
        last_episode = m.get('last_episode_to_air') or {}
        runtime = last_episode.get('runtime')
        episode_count = m.get('number_of_episodes')
        if not isinstance(episode_count, int):
            episode_count = last_episode.get('episode_number')
        if runtime and episode_count:
            runtime_tmdb = runtime * episode_count
    else:
        runtime_tmdb = m['runtime']
    movie_info["runtime"] = runtime_tmdb
    movie_info["runtime_tmdb"] = runtime_tmdb

    # 导演信息
    if tv:
        credits_list = m.get('credits', {})
        crew_list = credits_list.get('crew', [])
        movie_info["directors"] = [member.get('original_name') for member in crew_list if member.get('known_for_department') == 'Directing']
        movie_info["directors"].extend([member.get('name') for member in crew_list if member.get('known_for_department') == 'Directing'])
        original_names = [creator.get('original_name') for creator in m.get('created_by', [])]
        english_names = [creator.get('name') for creator in m.get('created_by', [])]
        movie_info["directors"].extend([name for name in original_names if name is not None])
        movie_info["directors"].extend([name for name in english_names if name is not None])
    else:
        cast_list = m.get('casts', {})
        crew_list = cast_list.get('crew', [])
        movie_info["directors"] = [member.get('original_name') for member in crew_list if member.get('job') == 'Director']
        movie_info["directors"].extend([member.get('name') for member in crew_list if member.get('job') == 'Director'])

    # 标题和译名信息
    translations_list = m.get('translations', {}).get('translations', [])
    m_key = 'name' if tv else 'title'
    alternative_titles = m.get('results' if tv else 'titles', [])
    movie_info["chinese_title"] = next((item['data'][m_key] for item in translations_list if item.get('iso_3166_1') == 'CN'), "")
    movie_info["titles"].extend([item['data'][m_key] for item in translations_list if item['data'][m_key]])
    movie_info["titles"].extend([item.get('title') for item in alternative_titles if item.get('title')])
    movie_info["titles"].append(m[m_key])
    movie_info["titles"].append(movie_info["original_title"])

    movie_info["poster_path"] = m['poster_path']


def get_imdb_movie_info(movie_id: str, movie_info: dict) -> None:
    """
    从本地 IMDb 镜像提取并合并影片元数据。

    :param movie_id: IMDb 电影编号
    :param movie_info: 电影信息字典，会在原对象上更新
    :return: 无
    """
    m = query_imdb_title_metadata(movie_id)
    if not m:
        logger.error(f"IMDb 本地库没有找到影片信息！{movie_id}")
        return

    # 基础字段
    if not movie_info["year"]:
        movie_info["year"] = m.get("start_year") or 0

    # 获取时长
    runtime_imdb = int(m.get("runtime_minutes") or 0)
    if not movie_info["runtime"]:
        movie_info["runtime"] = runtime_imdb
    movie_info["runtime_imdb"] = runtime_imdb

    # 获取标题和别名
    original_title = (m.get("original_title") or m.get("primary_title") or "").strip()
    if not movie_info["original_title"]:
        movie_info["original_title"] = original_title
    movie_info["titles"].extend(m.get("titles", []))

    # 获取类型和导演列表
    movie_info["genres"].extend(m.get("genres", []))
    movie_info["directors"].extend(m.get("directors", []))


def get_douban_movie_info(movie_id: str, movie_info: dict) -> None:
    """
    从 Douban 详情页提取并合并影片元数据。

    :param movie_id: Douban 电影编号
    :param movie_info: 电影信息字典，会在原对象上更新
    :return: 无
    """
    response = get_douban_response(movie_id, "movie_response")
    if not response:
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    info_div = soup.find("div", id="info")
    if not info_div:
        sys.exit(f"豆瓣页面解析失败")

    # 提取原始标题
    original_title = ""
    a_tag = soup.find('a', class_='nbgnbg')
    if a_tag:
        img_tag = a_tag.find('img')
        if img_tag and 'alt' in img_tag.attrs:
            original_title = img_tag['alt']
            movie_info["titles"].append(original_title)
            if not movie_info["original_title"]:
                movie_info["original_title"] = original_title

    # 提取中文标题
    chinese_title = soup.find("title").get_text(strip=True).replace("(豆瓣)", "").strip()
    if chinese_title != original_title:
        movie_info["titles"].append(chinese_title)
        if not movie_info["chinese_title"] and original_title and chinese_title != original_title:
            movie_info["chinese_title"] = chinese_title

    # 提取导演列表
    director_tag = info_div.find("span", class_="pl", string="导演")
    if director_tag:
        director_attrs = director_tag.find_next("span", class_="attrs")
        movie_info["directors"].extend([a.get_text(strip=True) for a in director_attrs.find_all("a")])

    # 提取类型标签
    genre_tags = info_div.find_all("span", property="v:genre")
    movie_info["genres"].extend([tag.get_text(strip=True) for tag in genre_tags])

    # 提取制片国家或地区
    country_tag = info_div.find("span", class_="pl", string="制片国家/地区:")
    if country_tag and country_tag.next_sibling:
        movie_info["country"].extend(
            [item.strip() for item in country_tag.next_sibling.strip().split("/") if item.strip()]
        )

    # 仅在前面没有拿到语言信息时才补豆瓣字段
    if not movie_info["language"]:
        language_tag = info_div.find("span", class_="pl", string="语言:")
        if language_tag and language_tag.next_sibling:
            movie_info["language"].extend(
                [item.strip() for item in language_tag.next_sibling.strip().split("/") if item.strip()]
            )

    # 提取年份
    if not movie_info["year"]:
        release_date_tag = info_div.find("span", property="v:initialReleaseDate")
        if release_date_tag:
            movie_info["year"] = release_date_tag.get_text(strip=True)[:4]

    # 提取片长
    if not movie_info["runtime"]:
        runtime_tag = info_div.find("span", property="v:runtime")
        if runtime_tag:
            movie_info["runtime"] = int(''.join(filter(str.isdigit, runtime_tag.get_text(strip=True))))

    # 提取别名列表
    aka_tag = info_div.find("span", class_="pl", string="又名:")
    if aka_tag and aka_tag.next_sibling:
        aka_name = aka_tag.next_sibling.strip()
        aka_names = aka_name.split("/")
        alias = [fix_douban_name(name) for name in aka_names]
        movie_info["titles"].extend(alias)
