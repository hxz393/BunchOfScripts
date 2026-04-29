"""
整理时用到的通用辅助函数

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import itertools
import json
import logging
import os
import os.path
import re
import shutil
import subprocess
from pathlib import Path
from typing import Dict, Iterable
from typing import Optional, Any

from my_module import read_json_to_dict, sanitize_filename, read_file_to_list, get_file_paths, remove_target, get_folder_paths

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict('config/sort_movie_ops.json')  # 配置文件

TRASH_LIST = CONFIG['trash_list']  # 垃圾文件名列表
MAGNET_PATH = CONFIG['magnet_path']  # 磁链前缀
BT_SOURCE = CONFIG['bt_source']  # BT 种子来源根目录
CHECK_TARGET = CONFIG['check_target']  # 种子移动目录
EVERYTHING_PATH = CONFIG['everything_path']  # everything 路径
MIRROR_PATH = CONFIG['mirror_path']  # 镜像文件夹路径
RU_PATH = CONFIG['ru_path']  # ru 种子路径
YTS_PATH = CONFIG['yts_path']  # yts 种子路径
DHD_PATH = CONFIG['dhd_path']  # dhd 种子路径
TTG_PATH = CONFIG['ttg_path']  # ttg 种子路径
SK_PATH = CONFIG['sk_path']  # ttg 种子路径
RARE_PATH = CONFIG['rare_path']  # rare 文件路径

BD_SOURCE = ['BDRemux', 'BluRay', 'BDRip']
# 编译正则，匹配文件名中包含 'yts' 且以 .jpg 或 .txt 结尾的文件（不区分大小写）
RE_TRASH = re.compile(r".*(yts|YIFY).*\.(jpg|txt)$", re.IGNORECASE)
IMDB_URL_PATTERN = re.compile(r"https?://(?:www\.)?imdb\.com/title/(tt\d+)", re.IGNORECASE)
IMDB_ID_PATTERN = re.compile(r"\b(tt\d+)\b", re.IGNORECASE)
IMDB_ID_TOKEN_PATTERN = re.compile(r"\btt\d+\b", re.IGNORECASE)
TORRENT_FILENAME_IMDB_PATTERN = re.compile(r"\[(tt\d+)]", re.IGNORECASE)


ID_MARKER_EXT_MAP = {'.tmdb': 'tmdb', '.douban': 'douban', '.imdb': 'imdb'}
EMPTY_ID_MARKERS: dict[str, Optional[str]] = {"imdb": None, "tmdb": None, "douban": None}
ID_MARKER_SUFFIXES = set(ID_MARKER_EXT_MAP.values())

YTS_TORRENT_PRIORITIES = (
    ("quality", ["2160p", "1080p", "720p", "480p", "3D"]),
    ("video_codec", ["x265", "x264"]),
    ("bit_depth", ["10", "8"]),
    ("type", ["bluray", "web"]),
)


def build_local_torrent_index(root_path: str | os.PathLike) -> dict[str, list[str]]:
    """
    扫描本地种子目录，并按文件名中的 IMDb 编号建立索引。

    只提取文件名里 ``[tt...]`` 形式的编号；父目录名中的编号不会参与索引。
    该函数用于程序启动阶段一次性扫描大量种子文件，避免后续每次查询 IMDb 编号时
    都线性遍历完整文件列表。

    :param root_path: BT 种子来源根目录
    :return: ``{imdb: [torrent_path, ...]}`` 格式的索引
    """
    root_path = os.fspath(root_path)
    index: dict[str, list[str]] = {}
    try:
        if not os.path.exists(root_path):
            logger.error(f"BT 种子来源目录不存在：{root_path}")
            return index
        if not os.path.isdir(root_path):
            logger.error(f"BT 种子来源路径不是目录：{root_path}")
            return index

        for root, _dirs, files in os.walk(root_path):
            for filename in files:
                imdb_ids = {
                    match.group(1).lower()
                    for match in TORRENT_FILENAME_IMDB_PATTERN.finditer(filename)
                }
                if not imdb_ids:
                    continue

                file_path = os.path.join(root, filename)
                for imdb_id in imdb_ids:
                    index.setdefault(imdb_id, []).append(file_path)
    except Exception:
        logger.exception(f"建立本地种子索引失败：{root_path}")
    return index


# 文件多，程序启动时先建立 IMDb -> 种子路径索引，后续查询避免反复扫描完整路径列表。
LOCAL_TORRENT_INDEX = build_local_torrent_index(BT_SOURCE)


def format_bytes(size_bytes: int | str) -> str:
    """
    将大小（字节）转换为可读的大小。

    :param size_bytes: 大小，字节
    :return: 可读的大小
    """
    size_bytes = int(size_bytes)
    size_name = ("B", "KB", "MB", "GB", "TB", "PB")
    i = 0
    while size_bytes >= 1024 and i < len(size_name) - 1:
        size_bytes /= 1024.
        i += 1
    return f"{size_bytes:.2f} {size_name[i]}"


def filter_torrents_by_priority(torrents: list[dict], key: str, priority_list: list[str]) -> list[dict]:
    """
    按字段优先级筛选 torrent 列表。

    先校验所有 ``torrent[key]`` 的值都在 ``priority_list`` 中；如果发现未知值，
    抛出 ``ValueError``，避免静默选错种子。校验通过后，按 ``priority_list``
    顺序返回第一个命中优先级对应的全部 torrent，并保留原列表顺序。

    :param torrents: torrent 字典列表，每个元素必须包含 ``key`` 字段
    :param key: 用于筛选的字段名，例如 ``quality``、``video_codec``、``bit_depth``、``type``
    :param priority_list: 允许值和优先级，越靠前优先级越高
    :return: 最高优先级命中的 torrent 列表；空输入返回空列表
    :raises KeyError: torrent 缺少 ``key`` 字段
    :raises ValueError: ``torrent[key]`` 出现 ``priority_list`` 之外的值
    """
    unique_values = {torrent[key] for torrent in torrents}
    unexpected_values = sorted(unique_values - set(priority_list))
    if unexpected_values:
        raise ValueError(f"Unexpected value for {key}: {unexpected_values}")

    for value in priority_list:
        filtered = [torrent for torrent in torrents if torrent[key] == value]
        if filtered:
            return filtered
    return torrents


def select_best_yts_magnet(json_data: dict, magnet_path: str) -> str:
    """
    从 YTS 电影详情 JSON 中选择最佳 torrent 并生成磁链。

    选择顺序依次为：
    1. 画质：2160p > 1080p > 720p > 480p > 3D
    2. 视频编码：x265 > x264
    3. 位深：10 > 8
    4. 来源：bluray > web
    5. 如果仍有多个候选，则取 ``size_bytes`` 最大者

    :param json_data: YTS movie details JSON，必须包含 ``data.movie.torrents``
    :param magnet_path: 磁链前缀，例如 ``magnet:?xt=urn:btih:``
    :return: 最佳 torrent 对应的磁链
    :raises KeyError: JSON 结构缺少必要字段，或 torrent 缺少 ``hash``、``size_bytes`` 等字段
    :raises ValueError: torrent 字段值超出允许优先级，或 ``torrents`` 为空
    """
    torrents = json_data["data"]["movie"]["torrents"]
    if not torrents:
        raise ValueError("YTS torrents is empty")

    for key, priority_list in YTS_TORRENT_PRIORITIES:
        torrents = filter_torrents_by_priority(torrents, key, priority_list)
        if len(torrents) == 1:
            return f"{magnet_path}{torrents[0]['hash']}"

    best_torrent = max(torrents, key=lambda torrent: int(torrent["size_bytes"]))
    return f"{magnet_path}{best_torrent['hash']}"


def build_unique_path(target_path: str | os.PathLike) -> Path:
    """
    为目标路径生成一个当前不存在的同目录路径。

    如果 ``target_path`` 不存在，直接返回该路径；如果已存在，则依次尝试
    ``name(1).ext``、``name(2).ext``，直到找到未被文件或目录占用的路径。
    本函数只计算路径，不创建文件，也不保证后续写入/移动操作的原子性。

    :param target_path: 原始目标路径
    :return: 当前未被占用的目标路径
    """
    path = Path(target_path)
    if not path.exists():
        return path

    base = path.stem
    suffix = path.suffix
    for index in itertools.count(1):
        candidate = path.with_name(f"{base}({index}){suffix}")
        if not candidate.exists():
            return candidate


def get_existing_id_files(path: str | os.PathLike) -> tuple[dict[str, Optional[str]], Optional[str]]:
    """
    扫描目录中的电影编号标记文件。

    支持的标记文件后缀由 ``ID_MARKER_EXT_MAP`` 定义：
    ``*.imdb``、``*.tmdb``、``*.douban``。函数把扩展名前的文件名作为编号值，
    不读取文件内容，也不校验文件是否为空。

    如果同一编号类型存在多个标记文件，返回空编号字典和错误信息；
    如果目录不存在，也返回空编号字典和错误信息。

    :param path: 待扫描目录路径
    :return: ``(编号字典, 错误信息)``。成功时错误信息为 ``None``；失败时编号字典三项均为 ``None``
    """
    id_files: dict[str, list[tuple[str, str]]] = {"imdb": [], "tmdb": [], "douban": []}
    directory = os.fspath(path)

    try:
        file_names = sorted(os.listdir(directory), key=str.casefold)
    except FileNotFoundError:
        return EMPTY_ID_MARKERS.copy(), f"目录不存在 {directory}"

    for file_name in file_names:
        if not os.path.isfile(os.path.join(directory, file_name)):
            continue

        name, ext = os.path.splitext(file_name)
        if ext.lower() in ID_MARKER_EXT_MAP and ext != ext.lower():
            return EMPTY_ID_MARKERS.copy(), f"目录 {directory} 中编号文件后缀必须小写：{file_name}"

        key = ID_MARKER_EXT_MAP.get(ext)
        if key:
            id_files[key].append((name, file_name))

    for key, values in id_files.items():
        if len(values) > 1:
            duplicate_names = [file_name for _name, file_name in values]
            return EMPTY_ID_MARKERS.copy(), f"目录 {directory} 中 {key.upper()} 编号文件太多，请先清理：{duplicate_names}"

    return {key: values[0][0] if values else None for key, values in id_files.items()}, None


def touch_id_marker(path: str | os.PathLike, id_value: str, suffix: str) -> None:
    """
    在目录中创建编号标记空文件。

    文件名格式为 ``{id_value}.{suffix}``，例如 ``tt1234567.imdb``。
    ``suffix`` 应使用不带点的小写标记类型：``imdb``、``tmdb``、``douban``。
    如果文件已存在，``Path.touch`` 会保留文件并更新修改时间。

    :param path: 标记文件所在目录
    :param id_value: 编号值
    :param suffix: 标记后缀，不含点
    :return: 无
    """
    if suffix not in ID_MARKER_SUFFIXES:
        raise ValueError(f"Unsupported ID marker suffix: {suffix}")
    Path(path, f"{id_value}.{suffix}").touch()


def remove_id_marker(path: str | os.PathLike, id_value: str, suffix: str) -> None:
    """
    删除目录中的指定编号标记文件。

    文件名格式为 ``{id_value}.{suffix}``。目标不存在时静默跳过；
    如果目标路径存在但不是文件，交由 ``Path.unlink`` 抛错。

    :param path: 标记文件所在目录
    :param id_value: 编号值
    :param suffix: 标记后缀，不含点
    :return: 无
    """
    if suffix not in ID_MARKER_SUFFIXES:
        raise ValueError(f"Unsupported ID marker suffix: {suffix}")
    marker_path = Path(path, f"{id_value}.{suffix}")
    marker_path.unlink(missing_ok=True)


def scan_ids(directory: str | os.PathLike) -> Dict[str, Optional[str]]:
    """
    扫描目录中的编号标记文件并返回编号字典。

    这是 ``get_existing_id_files`` 的容错包装：扫描失败、重复标记、
    或标记后缀大小写错误时，会记录错误日志，并返回空编号字典。

    :param directory: 待扫描目录路径
    :return: 包含 ``imdb``、``tmdb``、``douban`` 三个键的编号字典
    """
    result, error = get_existing_id_files(directory)
    if error:
        logger.error(error)
    return result


def remove_duplicates_ignore_case(items: Iterable[Any]) -> list[Any]:
    """
    按首次出现顺序去重。

    字符串使用 ``casefold()`` 后的值比较，因此大小写不同但文本相同的
    字符串只保留第一次出现的原始写法。可 hash 的非字符串按值比较；
    不可 hash 的对象使用 ``repr(item)`` 作为兜底比较键。

    :param items: 原始可迭代对象
    :return: 去重后的列表，元素顺序与首次出现顺序一致
    """
    seen = set()
    result = []
    for item in items:
        if isinstance(item, str):
            key = ("str", item.casefold())
        else:
            try:
                hash(item)
            except TypeError:
                key = ("repr", repr(item))
            else:
                key = ("value", item)
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def extract_imdb_id(text: str | None) -> Optional[str]:
    """
    从文本中提取第一个 IMDb title 编号。

    优先匹配标准 IMDb 标题页链接；找不到时回退到宽松的 ``tt`` 编号匹配。
    返回值统一转换为小写。若文本中包含多个编号，只返回第一个匹配结果。

    :param text: 输入文本，例如文件名、目录名、URL 或页面正文
    :return: IMDb title 编号，例如 ``tt1234567``；不存在时返回 ``None``
    """
    if not text:
        return None

    match = IMDB_URL_PATTERN.search(text)
    if match:
        return match.group(1).lower()

    match = IMDB_ID_PATTERN.search(text)
    return match.group(1).lower() if match else None


def parse_movie_id(movie_id: str | None) -> Optional[tuple[str, str]]:
    """
    将外部电影编号解析为数据库字段名和值。

    支持三种前缀格式：``tt...``、``tmdb...``、``db...``。
    函数只按前缀拆分，不校验编号主体是否为纯数字；无法识别的前缀返回 ``None``。

    :param movie_id: 原始电影编号，例如 ``tt1234567``、``tmdb123``、``db456``
    :return: ``(字段名, 字段值)``；字段名为 ``imdb``、``tmdb`` 或 ``douban``。无法识别时返回 ``None``
    """
    if not movie_id:
        return None

    movie_id = movie_id.strip()
    if movie_id.startswith("tt") and len(movie_id) > 2:
        return "imdb", movie_id.lower()
    if movie_id.startswith("tmdb") and len(movie_id) > 4:
        return "tmdb", movie_id[4:]
    if movie_id.startswith("db") and len(movie_id) > 2:
        return "douban", movie_id[2:]
    return None


def extract_imdb_id_from_links(hrefs: Iterable[str | None]) -> Optional[str]:
    """
    从链接序列中提取 IMDb title 编号。

    会遍历全部链接并优先返回标准 IMDb title URL 中的编号；
    如果没有标准 IMDb 链接，则回退到第一个能从链接文本中宽松提取出的 ``tt`` 编号。
    返回值统一为小写。

    :param hrefs: href 字符串序列
    :return: IMDb title 编号，例如 ``tt1234567``；不存在时返回 ``None``
    """
    fallback_imdb_id = None
    for href in hrefs:
        if not href:
            continue
        imdb_id = extract_imdb_id(href)
        if not imdb_id:
            continue
        if IMDB_URL_PATTERN.search(href):
            return imdb_id
        if fallback_imdb_id is None:
            fallback_imdb_id = imdb_id

    return fallback_imdb_id


def extract_torrent_download_link(target_path: str | os.PathLike, magnet_path: str) -> Optional[str]:
    """
    从下载记录文件中提取可提交给下载客户端的链接。

    支持两类来源：
    - ``.json``: YTS movie details JSON，读取 ``data.movie.torrents``，
      并调用 ``select_best_yts_magnet`` 按优先级生成 magnet。
    - ``.log``: 已保存的下载链接文本文件，只读取第一行并去除 UTF-8 BOM。

    读取失败、内容为空、JSON 结构不符合预期、无法生成 magnet、
    或文件后缀不支持时返回 ``None``。

    :param target_path: ``.json`` 或 ``.log`` 下载记录文件路径
    :param magnet_path: 生成 YTS magnet 时使用的前缀，例如 ``magnet:?xt=urn:btih:``
    :return: 下载链接；无法提取时返回 ``None``
    """
    file_path = Path(target_path)
    suffix = file_path.suffix.lower()

    if suffix == ".json":
        try:
            json_data = read_json_to_dict(file_path)
            if not json_data:
                logger.error(f"读取 JSON 失败: {file_path}")
                return None
            return select_best_yts_magnet(json_data, magnet_path)
        except Exception:
            logger.exception(f"从 JSON 提取下载链接失败: {file_path}")
            return None

    if suffix == ".log":
        lines = read_file_to_list(file_path)
        if not lines:
            logger.error(f"读取 LOG 失败或内容为空: {file_path}")
            return None
        download_link = lines[0].lstrip("\ufeff").strip()
        if not download_link:
            logger.error(f"读取 LOG 失败或内容为空: {file_path}")
            return None
        return download_link

    return None


def sort_torrents_auto(path: str) -> None:
    """
    简单整理下载目录中的 LOG 种子记录文件。

    ``path`` 下一级目录视为导演目录；导演目录下的直接子目录视为电影目录。
    LOG 文件按电影目录名是否出现在 LOG 文件名中匹配，匹配后移入目录。

    本函数是旧下载目录整理入口，只保留简单历史兼容逻辑，不做复杂模糊匹配。

    :param path: 下载来源根目录
    :return: 无返回值
    """
    logger.info(f"来源目录：{path}")
    for director_path in sorted((item for item in Path(path).iterdir() if item.is_dir()), key=lambda item: item.name.casefold()):
        film_paths = sorted((item for item in director_path.iterdir() if item.is_dir()), key=lambda item: item.name.casefold())
        for log_path in sorted((item for item in director_path.iterdir() if item.is_file() and item.suffix.lower() == ".log"), key=lambda item: item.name.casefold()):
            for film_path in film_paths:
                if film_path.name.casefold() not in log_path.name.casefold():
                    continue
                target_path = film_path / log_path.name
                shutil.move(str(log_path), str(target_path))
                logger.info(f"移动文件：{log_path} -> {target_path}")
                logger.info("-" * 255)
                break

    delete_trash_files(path)


def check_local_torrent(imdb: str) -> dict:
    """
    从本地种子索引中查找并移动指定 IMDb 编号的种子文件。

    函数从 ``LOCAL_TORRENT_INDEX`` 直接取出当前 IMDb 编号对应的候选路径；
    候选文件仍存在且文件名包含 ``[imdb]`` 时，将文件移动到 ``CHECK_TARGET``。
    目标目录已存在同名文件时，通过 ``build_unique_path`` 生成不重名路径，
    避免覆盖已有文件。

    ``LOCAL_TORRENT_INDEX`` 是模块导入时从 BT 来源目录建立的快照；函数不会重新
    扫描来源目录，因此运行期间新增的种子不会被本次检查发现。

    :param imdb: IMDb 编号，例如 ``tt1234567``
    :return: ``{"move_counts": 移动数量, "move_files": 移动后的路径列表}``
    """
    moved_files = []
    imdb = imdb.lower()
    bracket_id = f"[{imdb}]"
    target_dir = Path(CHECK_TARGET)
    target_dir.mkdir(parents=True, exist_ok=True)

    for file_path in LOCAL_TORRENT_INDEX.get(imdb, []):
        source_path = Path(file_path)
        if bracket_id not in source_path.name.lower() or not source_path.exists():
            continue

        target_path = build_unique_path(target_dir / source_path.name)
        shutil.move(str(source_path), str(target_path))
        moved_files.append(str(target_path))

    return {"move_counts": len(moved_files), "move_files": moved_files}


def merge_and_dedup(director_info: dict, result_info: dict) -> dict:
    """
    合并两个字典中相同键的列表，并去重（忽略大小写）。
    如果两个列表中有重复项（忽略大小写），保留第一次出现的值。

    :param director_info: 原字典
    :param result_info: 新字典
    :return: 返回合并后的字典。
    """
    merged = {}

    # 获取所有的键
    all_keys = set(director_info.keys()) | set(result_info.keys())
    for key in all_keys:
        list1 = director_info.get(key, [])
        list2 = result_info.get(key, [])
        combined = list1 + list2

        merged[key] = remove_duplicates_ignore_case(combined)

    return merged


def split_director_name(full_name: str) -> list:
    """
    根据规则，将输入字符串拆分为 [英文名, 中文名] 或 [中文名] 或 [原字符串].
    规则：
      1. 若不存在中文字符，则返回 [原字符串].
      2. 若存在中文字符，则视匹配到的不含空格、且至少含一个中文字符的片段为中文名。可能包含部分英文字母或符号（如 A·V·洛克威尔）。
      3. 将中文名从原字符串中移除后，剩余部分（若存在）即视为英文名。
      4. 若中英文名都存在，英文名在前，中文名在后。
      5. 若只有中文名，则只返回该中文名。

    :param full_name: 传入名字
    :return: 分割后的名字列表
    """
    # 匹配含有至少一个中文字符的片段（不包含空格）
    pattern = re.compile(r'\S*[\u4e00-\u9fff]+\S*')

    match = pattern.search(full_name)
    if not match:
        # 不包含中文，直接返回整个字符串
        return [full_name.strip()]

    # 如果包含中文
    chinese_part = match.group(0)
    # 将匹配到的中文名片段从原字符串中去掉，得到英文部分
    english_part = full_name.replace(chinese_part, '', 1).strip()

    if english_part:
        return [english_part, chinese_part]
    else:
        return [chinese_part]


def create_aka_director(path: str, aka: list) -> None:
    """
    写入导演别名到空白文件

    :param path: 导演目录路径
    :param aka: 别名列表
    :return: 返回文件名
    """
    # unique_aka = list(set(item.lower() for item in aka))
    seen = set()
    unique_aka = []
    for item in aka:
        lower_item = item.lower()  # 将当前项转换为小写
        if lower_item not in seen:
            seen.add(lower_item)  # 将小写形式添加到集合中
            unique_aka.append(item)  # 保留原始大小写的项
    for a in unique_aka:
        file_name = sanitize_filename(a).strip()
        Path(os.path.join(path, file_name)).touch()


def delete_trash_files(path: str) -> None:
    """
    删除垃圾文件

    :param path: 扫描路径
    :return: 无
    """
    trash_low = [i.lower() for i in TRASH_LIST]

    file_paths = get_file_paths(path)
    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        if file_name.lower() in trash_low:
            logger.info(f"删除垃圾：{remove_target(file_path)}")


def everything_search_filelist(file_path: str) -> None:
    """
    扫描所有文件

    :param file_path: 来源文本路径
    :return: 无
    """
    # 使用正则表达式方式，避免非全词匹配
    # keys = '|'.join(read_file_to_list(file_path))
    # search_query = f'({keys})'
    # command = f'"{EVERYTHING_PATH}" -regex -search "{search_query}"'
    # subprocess.run(command, shell=True)

    # 调用时直接把参数列表传给 Popen，避免管道符被 shell 解读
    search_query = "|".join(f"<{name}>" for name in read_file_to_list(file_path))
    subprocess.Popen([EVERYTHING_PATH, "-search", search_query], shell=False)


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


def fix_douban_name(name: str) -> str:
    """
    去除豆瓣名无关文字

    :param name: 豆瓣名
    :return: 合法的文件名
    """
    # 匹配模式：\([^)]*\) 匹配 ()，（[^）]*）匹配全角（）
    pattern = r'\([^)]*\)|（[^）]*）'
    result = re.sub(pattern, '', name)
    result = result.strip()
    return result


def extract_movie_ids(root_path):
    """
    遍历 root_path 下的每个导演文件夹，检查并提取其子文件夹名中 {} 内的 ID。
    如果遇到没有 {} 的文件夹，则报错并退出。
    """
    pattern = re.compile(r'\{([^}]*)}')  # 匹配 {内容}
    ids = []

    # 遍历一级子文件夹（导演名）
    for director in os.listdir(root_path):
        director_path = os.path.join(root_path, director)
        if not os.path.isdir(director_path):
            continue

        # 遍历二级子文件夹（影片文件夹）
        for film in os.listdir(director_path):
            film_path = os.path.join(director_path, film)
            if not os.path.isdir(film_path):
                continue

            match = pattern.search(film)
            if not match:
                # 直接报错并退出
                logger.error(f"Error: 影片文件夹 “{film}” 中不存在 {{}}，请检查命名格式。")
                return

            # 提取并收集 ID
            ids.append(match.group(1))

    return ids


def find_video_files(path):
    """获取所有视频文件的路径"""
    video_extensions = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv',
                        '.webm', '.m4v', '.mpg', '.mpeg', '.ts', '.3gp'}
    # 使用 rglob 递归遍历所有文件，检查后缀是否在集合中
    return [str(p) for p in Path(path).rglob('*') if p.suffix.lower() in video_extensions]


def filter_video_files(video_list):
    """根据文件名规则筛选出错误视频文件名"""
    result = []
    for p in video_list:
        name = Path(p).name  # 仅提取文件名（含扩展名）
        # 条件1：不包含 ex_ 且 不包含 SUB-
        cond1 = ("ex_" not in name) and ("SUB-" not in name)
        # 条件2：包含 (1)
        cond2 = "(1)" in name
        if cond1 or cond2:
            result.append(p)
    return result
