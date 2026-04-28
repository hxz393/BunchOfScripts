"""
整理时用到的通用辅助函数

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import contextlib
import itertools
import json
import logging
import os
import os.path
import re
import shutil
import subprocess
import warnings
from pathlib import Path
from typing import Dict, Iterable
from typing import Optional, Any

from PIL import Image
from moviepy import VideoFileClip

from my_module import read_json_to_dict, sanitize_filename, read_file_to_list, get_file_paths, remove_target, get_folder_paths

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict('config/sort_movie_ops.json')  # 配置文件

TRASH_LIST = CONFIG['trash_list']  # 垃圾文件名列表
SOURCE_LIST = CONFIG['source_list']  # 来源列表
VIDEO_EXTENSIONS = CONFIG['video_extensions']  # 后缀名列表
MAGNET_PATH = CONFIG['magnet_path']  # 磁链前缀
RARBG_SOURCE = CONFIG['rarbg_source']  # rarbg 种子来源路径
TTG_SOURCE = CONFIG['ttg_source']  # ttg 种子来源路径
DHD_SOURCE = CONFIG['dhd_source']  # dhd 种子来源路径
SK_SOURCE = CONFIG['sk_source']  # sk 种子来源路径
RARE_SOURCE = CONFIG['rare_source']  # rare 文件路径
RLS_SOURCE = CONFIG['rls_source']  # rare 文件路径
CHECK_TARGET = CONFIG['check_target']  # 种子移动目录
EVERYTHING_PATH = CONFIG['everything_path']  # everything 路径
FFPROBE_PATH = CONFIG['ffprobe_path']  # ffprobe 路径
MTM_PATH = CONFIG['mtm_path']  # mtm 路径
MEDIAINFO_PATH = CONFIG['mediainfo_path']  # mtm 路径
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
# 编译正则，从文件名中提取信息
RE_VIDEO_NAME = re.compile(
    r'^'  # 开头
    r'[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)*\.'  # 名称 (点号分隔单词)
    r'\d{4}\.'  # 年份 (4 位数字 + 点)
    r'(?:[A-Za-z0-9]+\.)?'  # 可选版本 (xxx.) — 非必须
    r'(?:480p|720p|1080p|2160p)\.'  # 分辨率 + 点
    r'(?:[A-Za-z0-9]+\.)?'  # 可选 site + 点
    r'[A-Za-z0-9\-]+\.'  # source + 点
    r'[A-Za-z0-9.@\-]+'  # encoding（允许字母数字和点号，以及 @ 之类） + 连字符
    r'-.+$'  # group (小组)
)
# 编译正则，便于复用
RE_JSON_FILE_NAME = re.compile(
    r'^'
    r'(?P<name>.*?)'  # 电影名 (尽量匹配少一点，直到遇到 '(' )
    r'\('  # 匹配左括号
    r'(?P<year>\d{4})'  # 4位年份
    r'\)'  # 匹配右括号
    r'\['  # 匹配 '['
    r'(?P<quality>.*?)'  # 质量 (用非贪心匹配，直到遇到 ']')
    r']'  # 匹配 ']'
    r'\{'  # 匹配 '{'
    r'(?P<id>.*?)'  # 编号 (用非贪心匹配，直到遇到 '}')
    r'}',  # 匹配 '}'
    re.IGNORECASE
)
IMDB_URL_PATTERN = re.compile(r"https?://(?:www\.)?imdb\.com/title/(tt\d+)", re.IGNORECASE)
IMDB_ID_PATTERN = re.compile(r"(tt\d+)", re.IGNORECASE)
IMDB_ID_TOKEN_PATTERN = re.compile(r"\btt\d+\b", re.IGNORECASE)

# 文件多，先行获取列表
PRE_LOAD_FP = get_file_paths(RARBG_SOURCE)
PRE_LOAD_FP.extend(get_file_paths(TTG_SOURCE))
PRE_LOAD_FP.extend(get_file_paths(DHD_SOURCE))
PRE_LOAD_FP.extend(get_file_paths(SK_SOURCE))
PRE_LOAD_FP.extend(get_file_paths(RARE_SOURCE))
PRE_LOAD_FP.extend(get_file_paths(RLS_SOURCE))

ID_MARKER_EXT_MAP = {'.tmdb': 'tmdb', '.douban': 'douban', '.imdb': 'imdb'}
EMPTY_ID_MARKERS: dict[str, Optional[str]] = {"imdb": None, "tmdb": None, "douban": None}
ID_MARKER_SUFFIXES = set(ID_MARKER_EXT_MAP.values())

YTS_TORRENT_PRIORITIES = (
    ("quality", ["2160p", "1080p", "720p", "480p", "3D"]),
    ("video_codec", ["x265", "x264"]),
    ("bit_depth", ["10", "8"]),
    ("type", ["bluray", "web"]),
)


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


def extract_imdb_ids(text: str) -> list[str]:
    """
    从文本中提取全部 IMDb 编号，并按首次出现顺序去重。

    :param text: 输入文本
    :return: IMDb 编号列表，例如 ``["tt1234567", "tt7654321"]``
    """
    if not text:
        return []

    return list(dict.fromkeys(match.lower() for match in IMDB_ID_TOKEN_PATTERN.findall(text)))


def extract_imdb_id(text: str) -> Optional[str]:
    """
    从文本中提取单个 IMDb 编号。

    优先匹配标准 IMDb 标题页链接；找不到时回退到宽松的 ``tt`` 编号匹配。

    :param text: 输入文本
    :return: IMDb 编号；不存在时返回 ``None``
    """
    if not text:
        return None

    match = IMDB_URL_PATTERN.search(text)
    if match:
        return match.group(1).lower()

    match = IMDB_ID_PATTERN.search(text)
    return match.group(1).lower() if match else None


def parse_movie_id(movie_id: str) -> Optional[tuple[str, str]]:
    """
    将电影编号解析为数据库字段名和对应值。

    支持三种格式：
    - ``tt...`` -> ``("imdb", "tt...")``
    - ``tmdb123`` -> ``("tmdb", "123")``
    - ``db456`` -> ``("douban", "456")``

    :param movie_id: 原始电影编号
    :return: ``(字段名, 字段值)``；无法识别时返回 ``None``
    """
    if movie_id.startswith("tt"):
        return "imdb", movie_id
    if movie_id.startswith("tmdb"):
        return "tmdb", movie_id[4:]
    if movie_id.startswith("db"):
        return "douban", movie_id[2:]
    return None


def extract_imdb_id_from_links(hrefs: Iterable[str]) -> Optional[str]:
    """
    从链接列表中提取 IMDb 编号。

    优先匹配标准 IMDb 标题页链接；找不到时回退到第一个宽松 ``tt`` 编号匹配。

    :param hrefs: 链接序列
    :return: IMDb 编号；不存在时返回 ``None``
    """
    fallback_imdb_id = None
    for href in hrefs:
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
    从来源文件中提取下载链接。

    目前支持：
    - ``.json``: YTS 电影详情 JSON，返回根据优先级挑选出的 magnet
    - ``.log``: 纯文本链接文件，返回首行去 BOM 后的链接

    :param target_path: 来源文件路径
    :param magnet_path: 生成 magnet 时使用的前缀
    :return: 提取到的下载链接；失败时返回 ``None``
    """
    file_path = Path(target_path)

    if file_path.suffix.lower() == ".json":
        json_data = read_json_to_dict(file_path)
        if not json_data:
            logger.error(f"读取 JSON 失败: {file_path}")
            return None
        try:
            return select_best_yts_magnet(json_data, magnet_path)
        except Exception:
            logger.exception(f"从 JSON 提取下载链接失败: {file_path}")
            return None

    if file_path.suffix.lower() == ".log":
        lines = read_file_to_list(file_path)
        if not lines:
            logger.error(f"读取 LOG 失败或内容为空: {file_path}")
            return None
        return lines[0].lstrip("\ufeff")

    return None


def safe_get(d: dict, path: list, default: Any = None) -> Any:
    """
    根据 path 列表，从字典 d 中逐层安全获取值。
    如果任意一步为 None 或不是 dict，就返回 default。

    :param d: 查询字典
    :param path: 查询键列表
    :param default: 出现问题时返回的默认值
    :return: 无
    """
    for key in path:
        if not isinstance(d, dict):
            return default
        d = d.get(key)
        if d is None:
            return default
    return d


def get_video_info(path_str: str) -> Optional[dict]:
    """
    在指定目录（包含子目录）中，寻找最大那个视频文件并返回其元数据信息：
      - resolution: "width*height" (例如 "1920x1080")
      - codec: 编解码器ID (如 "h264", "hevc" 等)
      - bitrate: 视频码率，返回形如 "2472kbps"

    :param path_str: 目录路径
    :return: 文件信息字典，有问题返回 None
    """
    # 获取合格视频文件路径
    largest_file_path = get_largest_file(path_str)
    if not largest_file_path:
        logger.error(f"没有找到任何视频文件：{path_str}")
        return
    # 获取视频文件信息
    return extract_video_info(largest_file_path)


def get_largest_file(path_str: str) -> str:
    """
    如果目录下有多个视频文件，返回最大的那个

    :param path_str: 扫描目录
    :return: 无
    """
    largest_file_path = ""
    largest_file_size = -1
    # 使用 os.walk 递归遍历子目录
    for root, dirs, files in os.walk(path_str):
        for filename in files:
            # 判断文件扩展名是否在常见视频扩展名列表中
            ext = os.path.splitext(filename)[1].lower()
            if ext in VIDEO_EXTENSIONS:
                filepath = os.path.join(root, filename)
                # 找到第一个视频文件后获取大小信息
                file_size = os.path.getsize(filepath)
                if file_size > largest_file_size:
                    largest_file_size = file_size
                    largest_file_path = filepath
    return largest_file_path


def extract_video_info(filepath: str) -> Optional[dict]:
    """
    调用 ffprobe 获取视频的分辨率、编解码器和码率信息
    返回一个字典：{"source": 来源, "resolution": 分辨率, "codec": 编码器, "bitrate": 比特率}

    :param filepath: 视频文件路径路径
    :return: 文件信息字典
    """
    logger.info(f"获取视频信息：{os.path.basename(filepath)}")
    dirname, filename = os.path.split(filepath)
    # 构造并运行 ffprobe 命令
    file_info = {"source": "", "resolution": "", "codec": "", "bitrate": ""}
    cmd = [
        FFPROBE_PATH,
        "-v", "quiet",  # 不输出调试信息
        "-print_format", "json",  # JSON格式输出
        "-show_format",
        "-show_streams",  # 显示所有流信息
        filepath
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')

    # 解析 JSON。通常第一个视频流在 streams[0]，也可能有音频流排在前面，需要做些过滤
    data = json.loads(result.stdout)
    video_stream = None
    for stream in data.get("streams", []):
        if stream.get("codec_type") == "video":
            video_stream = stream
            break
    if not video_stream:
        logger.error(f"未在文件 {filepath} 中检测到视频流")
        return

    # 分辨率，获取没有失败过
    width = video_stream.get("width", 0)
    height = video_stream.get("height", 0)
    file_info["resolution"] = f"{width}x{height}"
    file_info["quality"] = classify_resolution_by_pixels(f"{width}x{height}")

    # 实际宽高比
    file_info["dar"] = width / height
    if 'display_aspect_ratio' in video_stream:
        dar_str = video_stream['display_aspect_ratio']
        width, height = map(int, dar_str.split(':'))
        file_info["dar"] = width / height

    # 编码器，mkv 要特别判断
    codec_tag_string = video_stream.get("codec_tag_string", "未知编码器")
    codec_name = video_stream.get("codec_name", "未知编码器")
    codec_detail = check_video_codec(filepath)
    not_allow_codec = ["mpeg-4 visual", "mpeg video",
                       "Vimeo Encoder", "Zencoder Video Encoding System",
                       "VOLOHEVC", "ATEME Titan File", "ATEME Titan KFE",
                       "x264pro - Adobe CS Exporter Plug-in",
                       "TMPGEnc", "TMPGEnc MPEG Editor", "TMPGEnc XPress",
                       "Created by Nero",
                       ]
    not_allow_codec_short = ["Womble", "TMPGEnc", "HCenc"]
    if codec_detail and codec_detail not in not_allow_codec and not any([x in codec_detail for x in not_allow_codec_short]):
        file_info["codec"] = codec_detail
    elif codec_tag_string.startswith("["):
        file_info["codec"] = codec_name
    else:
        file_info["codec"] = codec_tag_string
    # 修剪名称
    if file_info["codec"].lower() in ["divx", "dx50", "div3"]:
        file_info["codec"] = "DivX"
    elif file_info["codec"].lower() == "xvid":
        file_info["codec"] = "XviD"
    elif file_info["codec"].lower() == "mpeg2video":
        file_info["codec"] = "mpeg2"
    file_info["codec"] = file_info["codec"][:49]
    file_info["codec"] = file_info["codec"].replace("x264pro - Adobe CS Exporter Plug-in", "x264")

    # 比特率，mkv 获取不到，改为获取总比特率
    bit_rate_bps = video_stream.get("bit_rate")
    if not bit_rate_bps:
        format_data = data.get("format", [])
        bit_rate_bps = format_data.get("bit_rate")
    bit_rate_kbps = int(bit_rate_bps) // 1000 if bit_rate_bps is not None else "未知比特率"
    file_info["bitrate"] = f"{bit_rate_kbps}kbps"

    # 视频时长
    duration = video_stream.get("duration")
    if not duration:
        duration_data = data.get("format", [])
        duration = duration_data.get("duration")
    file_info["duration"] = int(float(duration) / 60)

    # 视频来源，需要根据文件名判断。先仅使用当前视频文件名做匹配
    file_info["source"] = "未知"
    for source in SOURCE_LIST:
        # 使用 re.IGNORECASE 或在模式中用 (?i) 来忽略大小写
        # 注意 [A-Za-z] 仅排除英文字母，如果想排除数字可以改成 [A-Za-z0-9]
        pattern = rf"(?i)(?<![A-Za-z]){source}(?![A-Za-z])"
        if re.search(pattern, filepath):
            file_info["source"] = source
            break
    # 额外处理
    if "blu-ray" in filename.lower().replace(".", ' ') and "remux" in filename.lower().replace(".", ' '):
        file_info["source"] = "BDRemux"
    elif "bluray" in filename.lower().replace(".", ' ') and "remux" in filename.lower().replace(".", ' '):
        file_info["source"] = "BDRemux"
    elif "bd" in filename.lower().replace(".", ' ') and "remux" in filename.lower().replace(".", ' '):
        file_info["source"] = "BDRemux"
    elif "blu-ray" in filename.lower():
        file_info["source"] = "BluRay"
    elif "webdl" in filename.lower():
        file_info["source"] = "WEB-DL"

    # 发布组
    # from sort_movie_mysql import check_rls_grp, create_conn
    # conn = create_conn()
    # release_group = re.search(r'-(?P<group>[^-.]+)(?=\.[^.]+$)', os.path.basename(filepath))
    # if release_group:
    #     file_info["release_group"] = check_rls_grp(conn, release_group.group(1))
    file_info["release_group"] = ""

    # 文件名
    # file_info["filename"] = filename
    file_info["filename"] = ""

    # 注释
    match = re.search(r'「(.*?)」', os.path.basename(filepath))
    if match:
        file_info["comment"] = match.group(1)

    return file_info


def check_video_codec(path: str) -> Optional[str]:
    """使用 MediaInfo 获取编码信息"""
    # 调用 MediaInfo CLI 获取 JSON 元数据
    cmd = [MEDIAINFO_PATH, '--Output=JSON', path]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='replace')
    data = json.loads(proc.stdout)

    # 提取 Video track
    media = data.get('media', {})
    if not media:
        logger.warning(f'Mediainfo 解析 JSON 失败: {path}')
        return

    tracks = media.get('track', [])
    video = next((t for t in tracks if t.get('@type') == 'Video'), None)
    if not video:
        logger.warning(f'Mediainfo 未找到视频流: {path}')
        return

    # 编码器识别
    codec = video.get('Encoded_Library_Name') or video.get('Format', '').lower() or 'Unknown'

    # 解析编码设置: rc_mode / crf / bitrate
    enc_settings = video.get('Encoded_Library_Settings') or video.get('Encoded_Application', '')
    raw_rc = None
    crf_value = None
    target_bitrate = None  # kbps
    if enc_settings:
        for match in re.finditer(r"(rc|crf)=([\w.]+)", enc_settings):
            key, val = match.groups()
            if key == 'rc':
                raw_rc = val
            elif key == 'crf':
                try:
                    crf_value = int(float(val))
                except ValueError:
                    pass
        m = re.search(r"bitrate=(\d+)", enc_settings)
        if m:
            target_bitrate = int(m.group(1))

    # 构造 rc_mode 字符串
    rc_mode = None
    if crf_value is not None:
        rc_mode = f"crf{crf_value}"
    elif raw_rc and target_bitrate is not None:
        rc_mode = f"{raw_rc}"
    elif raw_rc:
        rc_mode = raw_rc

    return f"{codec}.{rc_mode}" if rc_mode else codec


def generate_video_contact_mtm(video_path: str) -> None:
    """
    用 mtn 生成视频网格缩略图，生成在视频同一目录

    :param video_path: 视频路径
    :return: 无
    """
    cmd = [
        MTM_PATH,  # 可使用原始字符串，避免转义
        "-c", "4",
        "-r", "4",
        "-h", "100",
        "-P",
        video_path
    ]

    logger.info(f"执行命令：{' '.join(cmd)}")
    subprocess.run(cmd, capture_output=True, text=True)


def generate_video_contact(video_path: str) -> None:
    """
    从视频中均匀抽取帧，生成一个网格缩略图，保持每个截图的原始宽高比。

    :param video_path: 视频文件路径
    :return 无返回值
    """
    logger.info(f"生成缩略图 {os.path.basename(video_path)}")
    # clip = VideoFileClip(video_path)
    with open(os.devnull, "w") as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            clip = VideoFileClip(video_path)

    # 视频存储的尺寸
    storage_width, storage_height = clip.size
    file_info = extract_video_info(video_path)
    dar = file_info.get("dar", 0)
    if not dar:
        dar = clip.aspect_ratio

    # 根据 DAR 手动计算显示宽度
    display_width = int(storage_height * dar)
    display_height = storage_height

    cols, rows = 4, 4
    total_images = cols * rows
    duration = clip.duration
    times = [duration * (i + 1) / (total_images + 1) for i in range(total_images)]

    images = []
    for t in times:
        frame = Image.fromarray(clip.get_frame(t).astype('uint8'))
        # 缩放到正确的显示尺寸（DAR）
        frame = frame.resize((display_width, display_height), Image.Resampling.LANCZOS)
        images.append(frame)

    grid_image = Image.new('RGB', (cols * display_width, rows * display_height))
    for idx, img in enumerate(images):
        col, row = idx % cols, idx // cols
        grid_image.paste(img, (col * display_width, row * display_height))

    output_path = os.path.splitext(video_path)[0] + "_s.jpg"
    grid_image.save(output_path)
    clip.close()


def classify_resolution_by_pixels(resolution: str) -> str:
    """
    根据宽和高的乘积（像素数）来给分辨率分类

    :param resolution: 分辨率字符串
    :return: 质量归类
    """
    w, h = resolution.split('x')
    w = int(w)
    h = int(h)

    pixel_count = w * h

    # 先把区间边界定义好，方便直观查看
    p240_max = 400 * 320  # = 128,000
    p480_max = 791 * 576  # = 442,368
    p720_max = 1280 * 960  # = 1,228,800
    p1080_max = 1950 * 1080  # = 2,106,000
    p2160_max = 3860 * 2160  # = 8,337,600

    if pixel_count <= p240_max:
        return "240p"
    elif pixel_count <= p480_max:
        return "480p"
    elif pixel_count <= p720_max:
        if h > 1000:
            return "1080p"
        if w > 1400:
            return "1080p"
        return "720p"
    elif pixel_count <= p1080_max:
        if h < 1000 and w < 1200:
            return "720p"
        return "1080p"
    elif pixel_count <= p2160_max:
        return "2160p"
    elif pixel_count > p2160_max:
        return "4320p"
    else:
        return "Unknown"


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


def sort_aka_files(source_path: str, target_path: str) -> None:
    """
    扫描来源目录下的空文件，移动到对应目标目录下

    :param source_path: 来源目录
    :param target_path: 目标目录
    :return: 无
    """
    logger.info(f"来源目录：{source_path}")
    logger.info(f"目标目录：{target_path}")
    # 获取所有子目录
    dir_dict = get_subdirs(source_path)
    for k, dir_path in dir_dict.items():
        p = Path(dir_path)
        destination_dir = Path(os.path.join(target_path, k))
        destination_dir.mkdir(parents=True, exist_ok=True)
        # if not destination_dir.exists():
        #     continue

        for path_item in p.iterdir():
            if path_item.is_file():
                if path_item.stat().st_size == 0:
                    dest_path = destination_dir / path_item.name
                    shutil.move(str(path_item), str(dest_path))
                    logger.info(f"移动：{str(path_item)} -> {str(dest_path)}")
                elif path_item.name == "movies.csv":
                    remove_target(str(path_item))
                    logger.info(f"删除：{str(path_item)}")


def sort_torrents_auto(path: str) -> None:
    """
    自动整理指定目录，扫描目录下的子目录，将其中下载完成的种子移动到对应目录

    :param path: 来源目录
    :return: 无
    """
    logger.info(f"来源目录：{path}")
    # 获取所有子目录，为导演目录
    dir_dict = get_subdirs(path)
    for k, dir_path in dir_dict.items():
        # 获取电影目录
        film_dict = get_subdirs(dir_path)
        if not film_dict:
            continue

        # 获取所有 json 文件
        json_dict = get_files_with_extensions(dir_path, ".json")
        # 获取所有 log 文件
        log_dict = get_files_with_extensions(dir_path, ".log")

        # 处理 json 文件，有一些不是通过盒子下载，电影目录为原名，要手动处理
        if json_dict:
            for json_name, json_path in json_dict.items():
                json_name_no_ext = os.path.splitext(json_name)[0]  # 正常文件名
                info_dict = parse_jason_file_name(json_name_no_ext)
                json_name_old = f"{info_dict.get('name')} ({info_dict.get('year')}) [{info_dict.get('id')}]"  # 旧文件名
                json_name_org = f"{info_dict.get('name')} ({info_dict.get('year')}) [{info_dict.get('quality')}]"  # 种子原始名
                names_to_check = [json_name_no_ext, json_name_old, json_name_org]
                names_to_check_alt = [n.translate(str.maketrans('', '', "'-,&")).replace("  ", " ") for n in names_to_check]
                tag_to_check = [info_dict.get('name'), info_dict.get('year'), info_dict.get('quality'), "yts"]  # 近似匹配，只限定来源 yts
                for film_name, film_path in film_dict.items():
                    if any(name.lower() in film_name.lower() for name in names_to_check_alt) or any(name.lower() in film_name.lower() for name in names_to_check) or all(sub.lower() in film_name.lower() for sub in tag_to_check):
                        target_path = os.path.join(film_path, json_name)
                        shutil.move(json_path, target_path)
                        logger.info(f"移动文件：{json_path} -> {target_path}")
                        os.rename(film_path, os.path.join(dir_path, json_name_no_ext))
                        logger.info(f"目录更名：{film_path} -> {json_name_no_ext}")
                        logger.info("-" * 255)
                        break

        # 处理 log 文件，将下载目录名去匹配种子名
        if log_dict:
            for log_name, log_path in log_dict.items():
                for film_name, film_path in film_dict.items():
                    # 判断目录名是否是文件名的子串，是就移动
                    if film_name in log_name:
                        target_path = os.path.join(film_path, log_name)
                        shutil.move(log_path, target_path)
                        logger.info(f"移动文件：{log_path} -> {target_path}")
                        logger.info("-" * 255)
                        break

    # 删除垃圾文件
    delete_trash_files(path)


def check_local_torrent(imdb: str) -> dict:
    """
    检查本地库存，将命中的种子移动到待检查目录。

    :param imdb: imdb 编号
    :return: 返回移动结果
    """
    result = {"move_counts": 0, "move_files": []}
    bracket_id = f"[{imdb}]"
    os.makedirs(CHECK_TARGET, exist_ok=True)

    file_paths = PRE_LOAD_FP
    for file_path in file_paths:
        if bracket_id in file_path:
            if not os.path.exists(file_path):
                # 文件可能已被删除，跳过
                continue

            target_path = str(build_unique_path(Path(CHECK_TARGET) / os.path.basename(file_path)))
            shutil.move(file_path, target_path)
            result["move_counts"] += 1
            result["move_files"].append(target_path)

    return result


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


def get_subdirs(dir_path: str) -> dict:
    """获取指定目录下所有直接子目录路径，不递归

    :param dir_path: 来源目录
    :return: 目录名和路径字典
    """
    result = {}
    for entry in os.listdir(dir_path):
        full_path = os.path.join(dir_path, entry)
        if os.path.isdir(full_path):
            result[entry] = full_path
    return result


def get_files_with_extensions(dir_path: str, extension: str) -> dict:
    """
    获取指定目录下所有符合扩展名的文件。

    :param dir_path: 来源目录
    :param extension: 扩展名
    :return: 目录名和路径字典
    """
    result = {}
    for entry in os.listdir(dir_path):
        full_path = os.path.join(dir_path, entry)
        if os.path.isfile(full_path):
            # 检查该文件是否匹配任意一个扩展名
            if entry.lower().endswith(extension):
                result[entry] = full_path
    return result


def parse_jason_file_name(filename: str) -> dict:
    """
    解析类似 "Sonic(2019)[1080p]{tt8108200}" 格式的文件名，
    返回一个字典，其中包含：
        - name: 电影名
        - year: 年份
        - quality: 视频质量(如 1080p)
        - id: 编号(如 tt8108200)

    :param filename: 不带扩展的文件名
    :return: 解析结果，匹配失败返回空字典
    """
    match = RE_JSON_FILE_NAME.match(filename)
    if not match:
        return {}

    return {
        'name': match.group('name').strip(),
        'year': match.group('year').strip(),
        'quality': match.group('quality').strip(),
        'id': match.group('id').strip(),
    }


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
