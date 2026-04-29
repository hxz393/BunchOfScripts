"""
整理时用到的通用辅助函数

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import itertools
import logging
import os
import os.path
import re
import shutil
import subprocess
from pathlib import Path
from typing import Dict, Iterable
from typing import Optional, Any

from my_module import read_json_to_dict, sanitize_filename, read_file_to_list, get_file_paths, remove_target

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict('config/sort_movie_ops.json')  # 配置文件

TRASH_LIST = CONFIG['trash_list']  # 垃圾文件名列表
MAGNET_PATH = CONFIG['magnet_path']  # 磁链前缀
CHECK_TARGET = CONFIG['check_target']  # 种子移动目录
EVERYTHING_PATH = CONFIG['everything_path']  # everything 路径
IMDB_URL_PATTERN = re.compile(r"https?://(?:www\.)?imdb\.com/title/(tt\d+)", re.IGNORECASE)
IMDB_ID_PATTERN = re.compile(r"\b(tt\d+)\b", re.IGNORECASE)
IMDB_ID_TOKEN_PATTERN = re.compile(r"\btt\d+\b", re.IGNORECASE)
CHINESE_NAME_SEGMENT_PATTERN = re.compile(r"\S*[\u4e00-\u9fff]+\S*")

ID_MARKER_EXT_MAP = {'.tmdb': 'tmdb', '.douban': 'douban', '.imdb': 'imdb'}
EMPTY_ID_MARKERS: dict[str, Optional[str]] = {"imdb": None, "tmdb": None, "douban": None}
ID_MARKER_SUFFIXES = set(ID_MARKER_EXT_MAP.values())

YTS_TORRENT_PRIORITIES = (
    ("quality", ["2160p", "1080p", "720p", "480p", "3D"]),
    ("video_codec", ["x265", "x264"]),
    ("bit_depth", ["10", "8"]),
    ("type", ["bluray", "web"]),
)
BT_SEARCH_ROOT = r"B:\0.整理\BT"


def search_local_torrents_by_imdb(imdb: str) -> list[str]:
    """
    使用 Everything 1.5 在本地 BT 目录中查询指定 IMDb 编号的文件。

    搜索范围固定为 ``B:\\0.整理\\BT``，只调用 ``es1.5`` 实例。Everything 使用
    一次正则查询同时匹配 ``[tt...]`` 和 ``{tt...}``；返回结果后，再按文件名
    二次过滤，避免父目录名命中导致误移动。

    :param imdb: IMDb 编号，例如 ``tt1234567``
    :return: 命中的完整文件路径列表
    :raises ValueError: ``imdb`` 不是合法 IMDb 编号
    :raises FileNotFoundError: 找不到 ``es.exe``
    :raises RuntimeError: Everything 查询失败
    """
    imdb = imdb.strip().lower()
    if not IMDB_ID_PATTERN.fullmatch(imdb):
        raise ValueError(f"无效 IMDb 编号：{imdb}")

    es_path = shutil.which("es.exe")
    if not es_path:
        raise FileNotFoundError("未找到 es.exe")

    imdb_tokens = (f"[{imdb}]", f"{{{imdb}}}")
    everything_regex = rf"[\[\{{]{re.escape(imdb)}[\]\}}]"
    command = [
        es_path,
        "-instance",
        "es1.5",
        "-r",
        everything_regex,
        "-full-path-and-name",
        "-path",
        BT_SEARCH_ROOT,
        "/a-d",
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        stderr_text = completed.stderr.strip()
        raise RuntimeError(f"Everything 查询失败：{stderr_text or completed.returncode}")

    results = []
    seen = set()
    for line in completed.stdout.splitlines():
        file_path = line.strip()
        if not file_path:
            continue
        file_name = Path(file_path).name.lower()
        if not any(token in file_name for token in imdb_tokens):
            continue
        path_key = file_path.casefold()
        if path_key in seen:
            continue
        seen.add(path_key)
        results.append(file_path)
    return results


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
    从本地 BT 目录中查找并移动指定 IMDb 编号的种子文件。

    函数通过 Everything 1.5 在 ``B:\\0.整理\\BT`` 中按 IMDb 编号查找候选路径。
    候选文件仍存在且文件名包含 ``[imdb]`` 或 ``{imdb}`` 时，将文件移动到 ``CHECK_TARGET``。
    目标目录已存在同名文件时，通过 ``build_unique_path`` 生成不重名路径，
    避免覆盖已有文件。

    Everything 负责维护文件索引，函数本身不再预加载或扫描完整 BT 目录。

    :param imdb: IMDb 编号，例如 ``tt1234567``
    :return: ``{"move_counts": 移动数量, "move_files": 移动后的路径列表}``
    """
    moved_files = []
    imdb = imdb.lower()
    imdb_tokens = (f"[{imdb}]", f"{{{imdb}}}")
    target_dir = Path(CHECK_TARGET)
    target_dir.mkdir(parents=True, exist_ok=True)

    for file_path in search_local_torrents_by_imdb(imdb):
        source_path = Path(file_path)
        file_name = source_path.name.lower()
        if not any(token in file_name for token in imdb_tokens) or not source_path.exists():
            continue

        target_path = build_unique_path(target_dir / source_path.name)
        shutil.move(str(source_path), str(target_path))
        moved_files.append(str(target_path))

    return {"move_counts": len(moved_files), "move_files": moved_files}


def split_director_name(full_name: str) -> list[str]:
    """
    将豆瓣人物页标题拆分为导演别名列表。

    如果标题中包含中文字符，函数把第一个包含中文字符的连续非空白片段视为中文名，
    并将剩余文本视为外文名；返回顺序固定为外文名在前、中文名在后。
    如果标题中没有中文字符，则返回清理空白后的原字符串。空输入返回空列表。

    :param full_name: 豆瓣人物页主标题，例如 ``John Smith 约翰·史密斯``
    :return: 拆分后的别名列表
    """
    full_name = full_name.strip()
    if not full_name:
        return []

    match = CHINESE_NAME_SEGMENT_PATTERN.search(full_name)
    if not match:
        return [full_name]

    chinese_part = match.group(0)
    english_part = full_name.replace(chinese_part, '', 1).strip()
    return [english_part, chinese_part] if english_part else [chinese_part]


def create_aka_director(path: str | os.PathLike, aka: Iterable[str]) -> None:
    """
    在导演目录中为每个别名创建空白标记文件。

    别名按大小写不敏感规则去重，保留第一次出现的原始写法；创建文件前会先
    通过 ``sanitize_filename()`` 清理非法文件名字符。清理后为空或重复的
    文件名会被跳过。已存在的文件会被 ``Path.touch()`` 保留并更新修改时间。

    :param path: 导演目录路径
    :param aka: 导演别名列表
    :return: 无
    """
    target_dir = Path(path)
    seen_file_names = set()
    for alias in remove_duplicates_ignore_case(aka):
        file_name = sanitize_filename(alias).strip()
        if not file_name:
            continue
        file_name_key = file_name.casefold()
        if file_name_key in seen_file_names:
            continue
        seen_file_names.add(file_name_key)
        Path(target_dir, file_name).touch()


def delete_trash_files(path: str | os.PathLike) -> None:
    """
    递归删除目录下文件名命中垃圾文件列表的文件。

    函数扫描 ``path`` 下所有文件，只按文件名与 ``TRASH_LIST`` 做大小写不敏感的
    精确匹配；命中后调用 ``remove_target()`` 删除。目录名不会参与匹配。

    :param path: 待扫描目录
    :return: 无
    """
    trash_names = {name.casefold() for name in TRASH_LIST}

    file_paths = get_file_paths(path)
    if not file_paths:
        return

    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        if file_name.casefold() in trash_names:
            logger.info(f"删除垃圾：{remove_target(file_path)}")


def open_everything_search_for_keywords(keywords: Iterable[str]) -> None:
    """
    用 Everything GUI 打开组合关键词搜索。

    每个关键词会包装为 Everything 全词搜索语法 ``<keyword>``，再用 ``|`` 连接成
    OR 查询。函数只负责打开搜索窗口，不读取搜索结果，也不等待搜索完成。

    :param keywords: 搜索关键词列表
    :return: 无
    """
    # 调用时直接把参数列表传给 Popen，避免管道符被 shell 解读
    search_query = "|".join(f"<{name}>" for name in keywords)
    subprocess.Popen([EVERYTHING_PATH, "-search", search_query], shell=False)


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
