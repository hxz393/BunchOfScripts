"""
整理时用到的通用辅助函数

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import contextlib
import json
import logging
import os
import os.path
import re
import shutil
import subprocess
import warnings
from pathlib import Path
from typing import Dict
from typing import Optional, Any

from PIL import Image
from moviepy import VideoFileClip

from my_module import read_json_to_dict, sanitize_filename, read_file_to_list, get_file_paths, remove_target, get_folder_paths
from scrapy_kpk import scrapy_kpk

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict('config/sort_movie_ops.json')  # 配置文件

TRASH_LIST = CONFIG['trash_list']  # 垃圾文件名列表
SOURCE_LIST = CONFIG['source_list']  # 来源列表
VIDEO_EXTENSIONS = CONFIG['video_extensions']  # 后缀名列表
MAX_BITRATE = CONFIG['max_bitrate']  # 最大比特率
MAGNET_PATH = CONFIG['magnet_path']  # 磁链前缀
RARBG_SOURCE = CONFIG['rarbg_source']  # rarbg 种子来源路径
TTG_SOURCE = CONFIG['ttg_source']  # ttg 种子来源路径
DHD_SOURCE = CONFIG['dhd_source']  # dhd 种子来源路径
SK_SOURCE = CONFIG['sk_source']  # sk 种子来源路径
RARE_SOURCE = CONFIG['rare_source']  # rare 文件路径
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
RE_NAME = re.compile(
    r'^'
    r'(?P<year>\d+)\s*-\s*'  # 放映年（4位数字）和分隔符
    r'(?P<title>[^{]+)'  # 电影原名：匹配除 { 和 ( 之外的字符
    r'(?:\((?P<chinese>[^)]+)\))?'  # 可选的电影中文名，包含在括号中
    r'\{(?P<imdb>(tt\d+|tmdb\d+|db\d+|noid)\d*(tv)?)}'  # IMDB 编号，形如 {tt1959550}
    r'\[(?P<source>[^]]+)]'  # 电影来源，例如 [DVDRip]
    r'\[(?P<resolution>[^]]+)]'  # 电影分辨率，例如 [656x368]
    r'\[(?P<encoding>[^]@]+)@(?P<bitrate>[^]]+)]'  # 文件编码和码率，例如 [XVID@1074kbps]
    r'$',
    re.IGNORECASE
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

# 文件多，先行获取列表
PRE_LOAD_FP = get_file_paths(RARBG_SOURCE)
PRE_LOAD_FP.extend(get_file_paths(TTG_SOURCE))
PRE_LOAD_FP.extend(get_file_paths(DHD_SOURCE))
PRE_LOAD_FP.extend(get_file_paths(SK_SOURCE))
PRE_LOAD_FP.extend(get_file_paths(RARE_SOURCE))


def get_ids(source_path: str) -> None:
    """
    解析给定的链接列表，根据不同站点（Douban、IMDB、TMDB）提取ID，并在输出目录中创建相应的空文件。

    :param source_path: 包含输出目录和链接列表的文本路径
    :return: 无
    """
    content = read_file_to_list(source_path)
    # 分割内容
    output_dir = content[0]
    if not os.path.exists(output_dir):
        logger.error("输出目录不存在！")
        return

    links_list = content[1:]
    for line in links_list:
        line = line.strip()
        if not line:
            continue  # 跳过空行

        # 处理 Douban
        if 'douban.com' in line:
            # 匹配形如 https://movie.douban.com/subject/1234567 或 https://www.douban.com/personage/xxxx
            match = re.search(r'https?://(?:movie\.|www\.)douban\.com/(?:subject|personage)/(\d+).*', line)
            if match:
                douban_id = match.group(1)
                out_file = os.path.join(output_dir, f"{douban_id}.douban")
                # 创建空文件（如果文件已存在则不改变内容）
                Path(out_file).touch()
        # 处理 IMDB
        elif 'imdb.com' in line:
            # 匹配形如 https://www.imdb.com/name/nm0396421/ 或 https://www.imdb.com/title/tt0012175/
            match = re.search(r'https?://www\.imdb\.com/(?:name|title)/([^/]+)/?.*', line)
            if match:
                imdb_id = match.group(1)
                out_file = os.path.join(output_dir, f"{imdb_id}.imdb")
                Path(out_file).touch()
        # 处理 TMDB
        elif 'themoviedb.org' in line:
            # 匹配形如 https://www.themoviedb.org/person/19032-john-hough 或 https://www.themoviedb.org/movie|tv/174171
            match = re.search(r'https?://www\.themoviedb\.org/(?:person|movie)/(\d+).*', line)
            match_tv = re.search(r'https?://www\.themoviedb\.org/tv/(\d+).*', line)
            if match:
                tmdb_id = match.group(1)
                out_file = os.path.join(output_dir, f"{tmdb_id}.tmdb")
                Path(out_file).touch()
            elif match_tv:
                tmdb_id = match_tv.group(1) + "tv"
                out_file = os.path.join(output_dir, f"{tmdb_id}.tmdb")
                Path(out_file).touch()


def scan_ids(directory: str) -> Dict[str, Optional[str]]:
    """
    扫描给定目录下的编号文件：
    - *.tmdb 文件保存tmdb编号
    - *.douban 文件保存douban编号
    - *.imdb 文件保存imdb编号
    如果某个文件未找到，则会打印提示信息。

    :param directory: 导演路径
    :return: 返回一个字典，包含可能的键：'tmdb', 'douban', 'imdb'
    """
    # 初始化变量为 None
    result = {'tmdb': None, 'douban': None, 'imdb': None}

    try:
        files = os.listdir(directory)
    except FileNotFoundError:
        logger.error(f"目录 {directory} 不存在。")
        return result

    # 检查是否有多个 id 文件
    ext_map = {'.tmdb': 'tmdb', '.douban': 'douban', '.imdb': 'imdb'}
    if any(value > 1 for value in {ext: sum(file.path.endswith(ext) for file in os.scandir(directory) if file.is_file()) for ext in ext_map}.values()):
        logger.error(f"目录 {directory} 中 id 文件太多，请先清理。")
        return result

    # 遍历目录中的文件
    for file in files:
        # 使用 os.path.splitext 分离文件名和扩展名
        name, ext = os.path.splitext(file)
        if ext in ext_map:
            result[ext_map[ext]] = name
    return result


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


def move_all_files_to_root(dir_path: str) -> None:
    """
    将目录下所有子文件夹(包括更深层次子文件夹)中的文件移动到根目录
    如果出现重名文件则自动加上数字编号
    提取文件完成后，删除所有空目录（递归搜索）

    :param dir_path: 指定目录
    :return: 无
    """
    dir_path = os.path.abspath(dir_path)

    # 移动所有子目录中的文件到根目录
    for root, dirs, files in os.walk(dir_path):
        # 如果当前遍历到的是 dir_path 本身，则跳过（不移动根目录下的文件）
        if root == dir_path:
            continue

        for file_name in files:
            old_file_path = os.path.join(root, file_name)
            # 目标文件路径：在 dir_path 下与 file_name 同名
            new_file_path = os.path.join(dir_path, file_name)

            # 如果 new_file_path 已经存在，则进行重命名
            if os.path.exists(new_file_path):
                base, ext = os.path.splitext(file_name)
                count = 1
                while True:
                    # 重新拼接新文件名，例如 "Traditional.chi(1).srt"
                    new_file_name = f"{base}({count}){ext}"
                    new_file_path = os.path.join(dir_path, new_file_name)
                    # 如果新路径还不存在，说明可以使用这个重命名
                    if not os.path.exists(new_file_path):
                        break
                    count += 1

            # 执行移动操作
            shutil.move(old_file_path, new_file_path)

    # 递归删除所有空目录（从最深层开始）
    for root, dirs, files in os.walk(dir_path, topdown=False):
        # 避免删除根目录
        if root == dir_path:
            continue
        # 如果目录为空，则删除
        if not os.listdir(root):
            os.rmdir(root)


def get_dl_link(path: str) -> str:
    """
    从文本文件中获取下载链接
    如果同时存在 json 和 log 文件，返回 log 文件中的磁链

    :param path: 电影目录
    :return: 磁力下载链接
    """
    _PATTERN = re.compile(r"magnet:\?xt=urn:btih:[A-Fa-f0-9]+", re.IGNORECASE)
    files = os.listdir(path)
    dl = None

    for file_name in files:
        file_path = os.path.join(path, file_name)
        if file_name.endswith('.json'):
            # 读取 json 文件，获取下载链接，重写文件然后删除 json 文件
            dl = select_yts_best_torrent(read_json_to_dict(file_path))
            output_path = os.path.splitext(file_path)[0] + ".log"
            with open(output_path, "w", encoding='utf-8') as f:
                f.write(dl)
            remove_target(file_path)
        elif file_name.endswith('.log'):
            # 在任意位置定位基础 Magnet 链接
            match = _PATTERN.search(read_file_to_list(file_path)[0].strip())
            dl = match.group(0) if match else None
            with open(file_path, "w", encoding='utf-8') as f:
                f.write(dl)

    return dl


def remove_duplicates_ignore_case(lst: list) -> list:
    """
    移除列表中重复的字符串元素，忽略大小写，保留第一个出现的版本。
    如果列表中的元素不是字符串，则直接比较（不进行大小写转换）。

    :param lst: 原始列表
    :return: 返回修改后的列表
    """
    seen = set()
    result = []
    for item in lst:
        # 如果是字符串，则转换为小写作为比较依据
        key = item.lower() if isinstance(item, str) else item
        if key not in seen:
            seen.add(key)
            result.append(item)
    # result.sort(key=lambda x: x.lower() if isinstance(x, str) else str(x))
    return result


def merged_dict(path: str, movie_info: dict, movie_ids: dict, file_info: dict) -> dict:
    """
    合并字典，去重

    :param path: 电影路径
    :param movie_info: 电影信息字典
    :param movie_ids: 电影编号字典
    :param file_info: 视频文件信息字典
    :return: 返回修改后的列表
    """
    movie_dict = movie_info | movie_ids | file_info
    movie_dict["director"] = Path(path).parent.name
    movie_dict["original_title"] = movie_dict["original_title"].replace("　", " ").replace("’", "'").replace("  ", " ")
    movie_dict["size"] = int(sum(file.stat().st_size for file in Path(path).rglob('*') if file.is_file()) / (1024 * 1024))
    movie_dict["dl_link"] = get_dl_link(path)
    movie_dict["year"] = int(movie_dict["year"]) if movie_dict["year"] else 0
    movie_dict["titles"].append(movie_dict["original_title"])
    # 针对字典中列表类型的字段去重（忽略大小写）
    for key in ['genres', 'country', 'language', 'titles', 'directors']:
        if key in movie_dict and isinstance(movie_dict[key], list):
            movie_dict[key] = remove_duplicates_ignore_case(movie_dict[key])

    return movie_dict


def get_movie_id(movie_dict: dict) -> str:
    """根据优先级返回电影的 ID，以加入到文件名中

    :param movie_dict: 电影信息字典
    :return: 电影的 ID
    """
    if movie_dict.get('imdb'):
        return movie_dict['imdb']
    elif movie_dict.get('tmdb'):
        return f"tmdb{movie_dict['tmdb']}"
    elif movie_dict.get('douban'):
        return f"db{movie_dict['douban']}"
    else:
        return "noid"


def build_movie_folder_name(path: str, movie_dict: dict) -> str:
    """
    生成电影文件夹名字

    :param path: 目录路径
    :param movie_dict: 电影信息字典
    :return: 返回文件名
    """
    # 使用 .get() 方法安全提取数据
    cn = movie_dict.get('chinese_title', '')
    en = movie_dict.get('original_title', '')
    yn = movie_dict.get('year', '')
    sc = movie_dict.get('source', '')
    rs = movie_dict.get('resolution', '')
    cd = movie_dict.get('codec', '')
    bt = movie_dict.get('bitrate', '')

    if not en:
        logger.error(f"没有获取到信息：{path}")
        base_name = os.path.basename(path)
    else:
        cn = "" if cn == en else cn
        movie_id = get_movie_id(movie_dict)
        base_name = f"{yn} - {en}{f'({cn})' if cn else ''}{{{movie_id}}}"

    return f"{base_name}[{sc}][{rs}][{cd}@{bt}]"


def create_aka_movie(new_path, movie_dict) -> None:
    """
    写入电影别名到空白文件

    :param new_path: 电影目录路径
    :param movie_dict: 电影信息字典
    :return: 返回文件名
    """
    if movie_dict["titles"]:
        for title in movie_dict["titles"]:
            file_name = sanitize_filename(title).strip()
            file_name = file_name.replace("\t", " ")
            file_name += ".别名"
            Path(os.path.join(new_path, file_name)).touch()


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
    not_allow_codec = ["mpeg-4 visual"]
    if codec_detail and codec_detail not in not_allow_codec:
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
    file_info["codec"] = file_info["codec"][:49]
    file_info["codec"] = file_info["codec"].replace("VOLOHEVC", "hevc").replace("Vimeo Encoder", "avc").replace("Zencoder Video Encoding System", "avc")

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

    # 视频来源，需要根据文件名判断
    matched = False
    # 先仅使用当前视频文件名做匹配
    for source in SOURCE_LIST:
        # 使用 re.IGNORECASE 或在模式中用 (?i) 来忽略大小写
        # 注意 [A-Za-z] 仅排除英文字母，如果想排除数字可以改成 [A-Za-z0-9]
        pattern = rf"(?i)(?<![A-Za-z]){source}(?![A-Za-z])"
        if re.search(pattern, filepath):
            file_info["source"] = source
            matched = True
            break
    # 匹配失败则扫描同级目录里的所有文件再尝试匹配（仅匹配log文件）
    if not matched:
        file_list = os.listdir(os.path.dirname(filepath))
        for f in file_list:
            if not f.endswith(".log"):
                continue
            for source in SOURCE_LIST:
                pattern = rf"(?i)(?<![A-Za-z]){source}(?![A-Za-z])"
                if re.search(pattern, f):
                    file_info["source"] = source
                    matched = True
                    break
            if matched:
                break  # 结束 file_list 循环

    if file_info["source"]:
        file_info["source"] = file_info["source"].replace("BrRip", "BDRip").replace("Blu-ray", "BluRay")
        file_info["source"] = file_info["source"].replace("WEB-DL", "WEB").replace("WEBDL", "WEB").replace("WEBRip", "WEB")

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


def check_movie(path: str) -> Optional[str]:
    """
    检查电影信息完整度流程

    :param path: 电影目录
    :return: 如有问题，返回问题
    """
    p = Path(path)
    file_list = [f for f in p.iterdir() if f.is_file()]

    # 检查信息文件
    movie_info_file = p / "movie_info.json5"
    if not os.path.exists(movie_info_file):
        return f"{p.name} 目录中不存在 movie_info.json5"
    movie_info = read_json_to_dict(movie_info_file)

    # 查找 log 文件数量
    log_paths = [str(f) for f in file_list if f.suffix.lower() == ".log"]
    if len(log_paths) > 1:
        return f"{p.name} 目录中下载数量大于 1"

    # 检查下载链接
    if len(log_paths) == 1:
        dl_link = read_file_to_list(log_paths[0])
        if len(dl_link[0]) != 60:
            return f"{p.name} 下载链接错误"

    # 检查导演是否正确
    if movie_info["director"].lower() not in [d.lower() for d in movie_info["directors"]]:
        logger.warning(f"{p.name} 导演 {movie_info['director']} 不在导演列表 {movie_info['directors']} 中")

    # 检查时长
    for source in ("imdb", "tmdb"):
        runtime_key = f"runtime_{source}"
        runtime_value = movie_info.get(runtime_key)
        if runtime_value:
            time_diff = abs(runtime_value - movie_info["duration"])
            if time_diff > 2:
                logger.warning(f"{source.upper()} 时长相差 {time_diff} 分钟。文件时长：{movie_info['duration']} 分钟，记录时长：{movie_info.get(runtime_key)} 分钟：{p.name} ")
            else:
                logger.info(f"{source.upper()} 时长匹配")
        else:
            logger.warning(f"{source.upper()} 时长缺失")

    # 检查其他字段信息
    for k, v in movie_info.items():
        if not v:
            if k not in ["chinese_title", "tmdb", "douban", "imdb", "size", "comment", "poster_path", "runtime_tmdb", "runtime_imdb"]:  # 能为空的字段
                logger.warning(f"{p.name} 缺少字段信息：{k}")

    # 查找多余目录
    dir_list = [f.name for f in p.iterdir() if f.is_dir()]
    if len(dir_list) != 0:
        return f"{p.name} 目录中有二级目录：{dir_list}"

    # 检查子目录是否符合规范
    match = RE_NAME.match(p.name)
    if not match:
        return f"{p.name} 目录名格式错误或缺少必须字段"

    # 检查本地库存
    imdb = movie_info['imdb']
    quality = movie_info['quality']
    source = movie_info['source']
    result = check_local_torrent(imdb, quality, source)
    move_counts = result['move_counts']
    delete_counts = result['delete_counts']
    if move_counts:
        logger.info(f"{imdb} 已删除本地库存文件 {delete_counts}：{result['delete_files']}")
        return f"{imdb} 请检查本地库存: {move_counts}"
    if delete_counts:
        logger.info(f"{imdb} 已删除本地库存文件 {delete_counts}：{result['delete_files']}")

    # 检查码率是否过高
    info = match.groupdict()
    file_bitrate = int(info['bitrate'].split('kbps')[0])
    high_bitrate = False
    if quality == '2160p' and file_bitrate > MAX_BITRATE * 20:
        high_bitrate = True
    elif quality == '1080p' and file_bitrate > MAX_BITRATE * 10:
        high_bitrate = True
    elif quality == '720p' and file_bitrate > MAX_BITRATE * 5:
        high_bitrate = True
    elif quality == '480p' and file_bitrate > MAX_BITRATE * 2:
        high_bitrate = True
    elif quality == '240p' and file_bitrate > MAX_BITRATE:
        high_bitrate = True
    if high_bitrate:
        logger.warning(f"{p.name} 码率过高：{file_bitrate}kbps")

    # 查找视频数量
    video_paths = [str(f) for f in file_list if f.suffix.lower() in VIDEO_EXTENSIONS]
    if len(video_paths) > 1:
        logger.warning(f"{p.name} 目录中视频数量大于 1")

    # 生成视频缩略图
    for video_path in video_paths:
        base, ext = os.path.splitext(video_path)
        screen_path = base + "_s.jpg"
        if not os.path.exists(screen_path):
            try:
                generate_video_contact(video_path)
            except Exception as e:
                logger.warning(f"{video_path} 生成缩略图失败: {e}")
        # 另一种生成缩略图方式
        if not os.path.exists(screen_path):
            generate_video_contact_mtm(video_path)
        if not os.path.exists(screen_path):
            return f"生成视频截图失败：{p.name}"

    # 检查在线科普库
    if quality not in ['1080p', '2160p'] and imdb:
        scrapy_kpk(imdb, quality)
        # scrapy_jeckett(imdb)

    # 建立镜像文件夹
    mirror_dir = Path(os.path.join(MIRROR_PATH, movie_info['director']))
    mirror_dir.mkdir(parents=True, exist_ok=True)

    # 删除垃圾文件
    delete_trash_files(path)


def check_local_torrent(imdb: str, quality: str, source: str) -> dict:
    """
    检查本地库存，如果质量大于 1080p 则删除库存，否则检查库存

    :param imdb: imdb 编号
    :param quality: 质量
    :param source: 来源
    :return: 返回检查结果
    """
    result = {"move_counts": 0, "delete_counts": 0, "delete_files": []}
    bracket_id = f"[{imdb}]"

    file_paths = PRE_LOAD_FP
    for file_path in file_paths:
        if bracket_id in file_path:
            if not os.path.exists(file_path):
                # 文件可能已被删除，跳过
                continue

            # 如果文件已经是 2160p 质量，直接删除库存种子，否则移动后处理
            if quality == '2160p':
                os.remove(file_path)
                result["delete_counts"] += 1
                result["delete_files"].append(file_path)
            elif quality == '1080p':
                if any(keyword in file_path.lower() for keyword in ("4k", "2160p", "uhd")):
                    target_path = os.path.join(CHECK_TARGET, os.path.basename(file_path))
                    shutil.move(file_path, target_path)
                    result["move_counts"] += 1
                else:
                    os.remove(file_path)
                    result["delete_counts"] += 1
                    result["delete_files"].append(file_path)
            else:
                target_path = os.path.join(CHECK_TARGET, os.path.basename(file_path))
                shutil.move(file_path, target_path)
                result["move_counts"] += 1

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

        seen = set()
        duped_list = []
        for item in combined:
            # 将字符串转换为小写进行比较
            lower_item = item.lower()
            if lower_item not in seen:
                seen.add(lower_item)
                duped_list.append(item)
        merged[key] = duped_list

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
    from sort_movie_mysql import create_conn, get_movie_batch, get_wanted_batch
    conn = create_conn()
    # 获取种子列表
    file_path_list = get_file_paths(DHD_PATH) + get_file_paths(TTG_PATH) + get_file_paths(SK_PATH) + get_file_paths(RARE_PATH)
    # 扫描所有文件得到 imdb_id_set
    imdb_id_set = {m.group(1) for f in file_path_list if (m := re.search(r'(tt\d+)', f))}

    # 批量取出数据
    wanted_rows = get_wanted_batch(conn, imdb_id_set)
    movie_rows = get_movie_batch(conn, imdb_id_set)

    wanted_map = {row['imdb']: row for row in wanted_rows}
    movie_map = {row['imdb']: row for row in movie_rows}
    for file_path in file_path_list:
        # 获取 IMDB 编号，不存在则跳过
        imdb_id = m.group(1) if (m := re.search(r'(tt\d+)', file_path)) else None
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


def select_yts_best_torrent(json_data: dict) -> str:
    """从 yts json 中选择最佳的下载"""
    torrents = json_data['data']['movie']['torrents']

    def filter_or_raise(candidates: dict, key: str, priority_list: list) -> [list | dict]:
        """辅助函数，如果有意外的值则抛出异常"""
        unique_values = set(t[key] for t in candidates)
        if unique_values - set(priority_list):
            raise ValueError(f"Unexpected value for {key}: {unique_values - set(priority_list)}")
        for val in priority_list:
            filtered = [t for t in candidates if t[key] == val]
            if filtered:
                return filtered
        return candidates

    # 逐步过滤
    torrents = filter_or_raise(torrents, 'quality', ['2160p', '1080p', '720p', '480p', '3D'])
    if len(torrents) == 1:
        return f"{MAGNET_PATH}{torrents[0]['hash']}"

    torrents = filter_or_raise(torrents, 'video_codec', ['x265', 'x264'])
    if len(torrents) == 1:
        return f"{MAGNET_PATH}{torrents[0]['hash']}"

    torrents = filter_or_raise(torrents, 'bit_depth', ['10', '8'])
    if len(torrents) == 1:
        return f"{MAGNET_PATH}{torrents[0]['hash']}"

    torrents = filter_or_raise(torrents, 'type', ['bluray', 'web'])
    if len(torrents) == 1:
        return f"{MAGNET_PATH}{torrents[0]['hash']}"

    # 最后根据文件大小确定
    best_torrent = max(torrents, key=lambda t: t['size_bytes'])
    return f"{MAGNET_PATH}{best_torrent['hash']}"


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
