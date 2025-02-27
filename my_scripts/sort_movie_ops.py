"""
整理时用到的通用辅助函数

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""

import json
import logging
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Dict
from typing import Optional, Any

from my_module import read_json_to_dict, sanitize_filename, read_file_to_list

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict('config/sort_movie_ops.json')  # 配置文件

SOURCE_LIST = CONFIG['source_list']  # 来源列表
VIDEO_EXTENSIONS = CONFIG['video_extensions']  # 后缀名列表
MAX_BITRATE = CONFIG['max_bitrate']  # 最大比特率
MAGNET_PATH = CONFIG['magnet_path']  # 磁链前缀
RARBG_SOURCE = CONFIG['rarbg_source']  # rarbg 种子来源路径
RARBG_TARGET = CONFIG['rarbg_target']  # rarbg 种子移动目录

# 编译正则，匹配文件名中包含 'yts' 且以 .jpg 或 .txt 结尾的文件（不区分大小写）
RE_TRASH = re.compile(r".*(yts|YIFY).*\.(jpg|txt)$", re.IGNORECASE)
# 编译正则，从文件名中提取信息
RE_NAME = re.compile(
    r'^'
    r'(?P<year>\d{4})\s*-\s*'  # 放映年（4位数字）和分隔符
    r'(?P<title>[^{]+)'  # 电影原名：匹配除 { 和 ( 之外的字符
    r'(?:\((?P<chinese>[^)]+)\))?'  # 可选的电影中文名，包含在括号中
    r'\{(?P<imdb>(tt\d+|tmdb\d+|db\d+)\d+)}'  # IMDB 编号，形如 {tt1959550}
    r'\[(?P<source>[^]]+)]'  # 电影来源，例如 [DVDRip]
    r'\[(?P<resolution>[^]]+)]'  # 电影分辨率，例如 [656x368]
    r'\[(?P<encoding>[^]@]+)@(?P<bitrate>[^]]+)]'  # 文件编码和码率，例如 [XVID@1074kbps]
    r'$',
    re.IGNORECASE
)


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
            match = re.search(r'https?://www\.themoviedb\.org/(?:person|movie|tv)/(\d+).*', line)
            if match:
                tmdb_id = match.group(1)
                out_file = os.path.join(output_dir, f"{tmdb_id}.tmdb")
                Path(out_file).touch()


def scan_ids(directory: str) -> Dict[str, Optional[str]]:
    """
    扫描给定目录下的导演编号文件：
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
        print(f"目录 {directory} 不存在。")
        return result

    # 遍历目录中的文件
    for file in files:
        # 使用 os.path.splitext 分离文件名和扩展名
        name, ext = os.path.splitext(file)
        # ext 包含点，例如 ".tmdb"
        if ext == '.tmdb':
            result['tmdb'] = name
        elif ext == '.douban':
            result['douban'] = name
        elif ext == '.imdb':
            result['imdb'] = name
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
    files = os.listdir(path)
    dl = None
    for file_name in files:
        file_path = os.path.join(path, file_name)
        if file_name.endswith('.json'):
            # 读取 json 文件，获取下载链接
            dl_info = {t['size_bytes']: t['hash'] for t in read_json_to_dict(file_path)['data']['movie']['torrents']}
            dl = f"{MAGNET_PATH}{dl_info[max(dl_info.keys())]}[:255]"
        elif file_name.endswith('.log'):
            return read_file_to_list(file_path)[0][:255]

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
        return "tt0000000"


def gen_folder_name(path: str, movie_dict: dict) -> str:
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
            Path(os.path.join(new_path, file_name).replace("\"", "")).touch()


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
    # 构造并运行 ffprobe 命令
    file_info = {"source": "未知来源", "resolution": "", "codec": "", "bitrate": ""}
    cmd = [
        r"D:\Software\Portable\PortableApps\弹弹play\ffmpeg\ffprobe.exe",
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

    # 编码器，mkv 要特别判断
    codec_tag_string = video_stream.get("codec_tag_string", "未知编码器").upper()
    codec_name = video_stream.get("codec_name", "未知编码器").upper()
    file_info["codec"] = codec_name if codec_tag_string == "[0][0][0][0]" else codec_tag_string

    # 比特率，mkv 获取不到，改为获取总比特率
    bit_rate_bps = video_stream.get("bit_rate")
    if not bit_rate_bps:
        format_data = data.get("format", [])
        bit_rate_bps = format_data.get("bit_rate")
    bit_rate_kbps = int(bit_rate_bps) // 1000 if bit_rate_bps is not None else "未知比特率"
    file_info["bitrate"] = f"{bit_rate_kbps}kbps"

    # 视频来源，需要根据文件名判断
    matched = False
    # 先仅使用当前视频文件名做匹配
    for source in SOURCE_LIST:
        # 使用 re.IGNORECASE（或在模式中用 (?i) ）来忽略大小写
        # 注意 [A-Za-z] 仅排除英文字母，如果想排除数字可以改成 [A-Za-z0-9]
        pattern = rf"(?i)(?<![A-Za-z]){source}(?![A-Za-z])"
        if re.search(pattern, filepath):
            file_info["source"] = source
            matched = True
            break
    # 匹配失败则扫描同级目录里的所有文件再尝试匹配
    if not matched:
        file_list = os.listdir(os.path.dirname(filepath))
        for f in file_list:
            for source in SOURCE_LIST:
                pattern = rf"(?i)(?<![A-Za-z]){source}(?![A-Za-z])"
                if re.search(pattern, f):
                    file_info["source"] = source
                    matched = True
                    break
            if matched:
                break  # 结束 file_list 循环

    file_info["source"] = file_info["source"].replace("BrRip", "BDRip").replace("Blu-ray", "BluRay")
    return file_info


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
    p480_max = 768 * 576  # = 442,368
    p720_max = 1280 * 960  # = 1,228,800
    p1080_max = 1950 * 1080  # = 2,106,000
    p2160_max = 3860 * 2160  # = 8,337,600

    if pixel_count <= p240_max:
        return "240p"
    elif pixel_count <= p480_max:
        return "480p"
    elif pixel_count <= p720_max:
        return "720p"
    elif pixel_count <= p1080_max:
        return "1080p"
    elif pixel_count <= p2160_max:
        return "2160p"
    elif pixel_count > p2160_max:
        return "4320p"
    else:
        return "Unknown"


def check_folder(path: str) -> Optional[str]:
    """
    检查流程

    :param path: 电影目录
    :return: 如有问题，返回问题
    """
    p = Path(path)

    # 检查信息文件
    movie_info_file = p / "movie_info.json5"
    if not os.path.exists(movie_info_file):
        return f"目录中不存在 movie_info.json"
    movie_info = read_json_to_dict(movie_info_file)

    # 检查导演是否正确
    if movie_info["director"] not in movie_info["directors"]:
        logger.warning(f"导演 {movie_info['director']} 不在导演列表 {movie_info['directors']} 中")

    # 查找多余目录
    dir_list = [f.name for f in p.iterdir() if f.is_dir()]
    if len(dir_list) != 0:
        return f"目录中有二级目录：{dir_list}"

    # 查找垃圾文件
    file_list = [f for f in p.iterdir() if f.is_file()]
    trash_files = [f.name for f in file_list if RE_TRASH.search(f.name)]
    if trash_files:
        return f"目录中有垃圾文件：{trash_files}"

    # 查找视频数量
    video_files = [f.name for f in file_list if f.suffix.lower() in VIDEO_EXTENSIONS]
    if len(video_files) > 1:
        logger.warning(f"目录中视频数量大于 1")

    # 检查子目录是否符合规范
    match = RE_NAME.match(p.name)
    if not match:
        return f"目录名格式错误或缺少必须字段"

    # 检查码率是否过高
    info = match.groupdict()
    file_bitrate = int(info['bitrate'].split('kbps')[0])
    if file_bitrate > MAX_BITRATE:
        logger.warning(f"码率过高：{file_bitrate}kbps")

    # 检查 RARBG 库存
    imdb = movie_info['imdb']
    quality = movie_info['quality']
    bracket_id = f"[{imdb}]"
    delete_counts = 0
    move_counts = 0
    for filename in os.listdir(RARBG_SOURCE):
        if bracket_id in filename:
            source_file = os.path.join(RARBG_SOURCE, filename)
            dest_file = os.path.join(RARBG_TARGET, filename)
            # 如果文件已经是 1080p 以上质量，直接删除库存种子，否则移动后处理
            if quality == '1080p' or quality == '2160p':
                os.remove(source_file)
                delete_counts += 1
            else:
                shutil.move(source_file, dest_file)
                move_counts += 1
    if move_counts:
        logger.warning(f"请检查 RARBG 库存: {move_counts}")
    if delete_counts:
        print(f"已删除 RARBG 库存文件 {delete_counts}")

    return None


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
        Path(os.path.join(path, file_name).replace("\"", "")).touch()
