"""
视频信息读取和截图生成工具。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import contextlib
import io
import json
import logging
import os
import re
import subprocess
import warnings
from pathlib import Path
from typing import Any, Optional

from PIL import Image, ImageDraw, ImageFont
from moviepy import VideoFileClip

from my_module import read_json_to_dict

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict('config/sort_movie_ops.json')  # 配置文件

SOURCE_LIST = CONFIG['source_list']  # 来源列表
VIDEO_EXTENSIONS = CONFIG['video_extensions']  # 后缀名列表
VIDEO_EXTENSION_SET = {extension.lower() for extension in VIDEO_EXTENSIONS}
FFPROBE_PATH = CONFIG['ffprobe_path']  # ffprobe 路径
FFMPEG_PATH = CONFIG.get('ffmpeg_path') or (str(Path(FFPROBE_PATH).with_name("ffmpeg.exe")) if FFPROBE_PATH else "ffmpeg")  # ffmpeg 路径
MTN_PATH = CONFIG['mtn_path']  # mtn 路径
MEDIAINFO_PATH = CONFIG['mediainfo_path']  # mediainfo 路径

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

FFPROBE_TIMEOUT_SECONDS = 60
FFMPEG_TIMEOUT_SECONDS = 120
MEDIAINFO_TIMEOUT_SECONDS = 60
MTN_TIMEOUT_SECONDS = 120
VIDEO_CONTACT_COLUMNS = 4
VIDEO_CONTACT_ROWS = 4
MTN_CONTACT_THUMB_HEIGHT = 100
VIDEO_CONTACT_TIMESTAMP_MIN_FONT_SIZE = 56
VIDEO_CONTACT_TIMESTAMP_FONT_DIVISOR = 10
VIDEO_CONTACT_TIMESTAMP_TEXT_COLOR = (235, 235, 235, 255)
VIDEO_CONTACT_TIMESTAMP_BG_COLOR = (0, 0, 0, 165)
VIDEO_CONTACT_TIMESTAMP_STROKE_COLOR = (0, 0, 0, 255)
VIDEO_CONTACT_TIMESTAMP_FONT_NAMES = ("arialbd.ttf", "segoeuib.ttf", "arial.ttf", "segoeui.ttf", "msyh.ttc", "simhei.ttf")
VIDEO_CONTACT_HDR_TRANSFERS = {"smpte2084", "arib-std-b67"}
VIDEO_CONTACT_HDR_PRIMARIES = {"bt2020"}
VIDEO_CONTACT_SDR_TRANSFERS = {"bt709", "smpte170m", "bt470bg", "iec61966-2-1"}
VIDEO_CONTACT_HDR_TONEMAP_FILTER = (
    "zscale=t=linear:npl=100,"
    "format=gbrpf32le,"
    "tonemap=tonemap=mobius:param=0.3:desat=0,"
    "zscale=t=bt709:m=bt709:p=bt709,"
    "format=rgb24"
)
IGNORED_CODEC_DETAILS = {
    "mpeg-4 visual",
    "mpeg video",
    "Vimeo Encoder",
    "Zencoder Video Encoding System",
    "VOLOHEVC",
    "ATEME Titan File",
    "ATEME Titan KFE",
    "x264pro - Adobe CS Exporter Plug-in",
    "TMPGEnc",
    "TMPGEnc MPEG Editor",
    "TMPGEnc XPress",
    "Created by Nero",
}
IGNORED_CODEC_DETAIL_SUBSTRINGS = ("Womble", "TMPGEnc", "HCenc")
CODEC_ALIASES = {
    "divx": "DivX",
    "dx50": "DivX",
    "div3": "DivX",
    "xvid": "XviD",
    "mpeg2video": "mpeg2",
}


def get_video_info(path_str: str | os.PathLike) -> Optional[dict]:
    """
    读取电影目录中最大视频文件的基础媒体信息。

    函数会递归扫描 ``path_str`` 下所有受支持的视频后缀文件，
    选择文件体积最大的一个作为正片候选，然后调用 ``extract_video_info``
    读取分辨率、清晰度、编码、码率、时长、来源等字段。

    没有找到视频文件，或视频信息提取失败时返回 ``None``。

    :param path_str: 电影目录路径
    :return: 视频信息字典；无法提取时返回 ``None``
    """
    largest_file_path = get_largest_file(path_str)
    if not largest_file_path:
        logger.error(f"没有找到任何视频文件：{path_str}")
        return None

    try:
        video_info = extract_video_info(largest_file_path)
    except Exception:
        logger.exception(f"读取视频信息失败：{largest_file_path}")
        return None

    if not video_info:
        logger.error(f"读取视频信息失败：{largest_file_path}")
        return None
    return video_info


def get_largest_file(path_str: str | os.PathLike) -> Optional[str]:
    """
    递归查找目录中体积最大的视频文件。

    只统计扩展名存在于 ``VIDEO_EXTENSIONS`` 的文件，扩展名比较不区分大小写。
    如果多个视频文件大小相同，按遍历顺序保留先遇到的文件。
    未找到视频文件时返回 ``None``。

    :param path_str: 待扫描目录路径
    :return: 最大视频文件路径；不存在视频文件时返回 ``None``
    """
    largest_file_path = None
    largest_file_size = -1
    scan_root = os.fspath(path_str)
    for root, dirs, files in os.walk(scan_root):
        dirs.sort(key=str.casefold)
        files.sort(key=str.casefold)
        for filename in files:
            ext = os.path.splitext(filename)[1].lower()
            if ext not in VIDEO_EXTENSION_SET:
                continue

            filepath = os.path.join(root, filename)
            try:
                file_size = os.path.getsize(filepath)
            except OSError as e:
                logger.warning(f"获取视频文件大小失败，跳过：{filepath}: {e}")
                continue

            if file_size > largest_file_size:
                largest_file_size = file_size
                largest_file_path = filepath
    return largest_file_path


def extract_video_info(filepath: str | os.PathLike) -> Optional[dict]:
    """
    使用 ffprobe 和 MediaInfo 提取单个视频文件的整理元数据。

    函数会读取第一个视频流，返回整理和入库所需的本地媒体字段：
    ``resolution``、``quality``、``dar``、``codec``、``bitrate``、
    ``duration``、``source``、``release_group``、``filename``，以及可选的
    ``comment``。其中 ``source`` 主要根据文件名和路径中的来源标记推断。

    找不到视频流，或关键媒体字段无法解析时返回 ``None``。

    :param filepath: 视频文件路径
    :return: 视频信息字典；无法解析时返回 ``None``
    """
    filepath = os.fspath(filepath)
    logger.info(f"获取视频信息：{os.path.basename(filepath)}")
    _dirname, filename = os.path.split(filepath)
    file_info = {"source": "", "resolution": "", "codec": "", "bitrate": ""}

    # 构造并运行 ffprobe 命令。
    cmd = [
        FFPROBE_PATH,
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        filepath
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=FFPROBE_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        logger.exception(f"ffprobe 执行超时：{filepath}")
        return None
    except OSError:
        logger.exception(f"ffprobe 执行失败：{filepath}")
        return None

    if getattr(result, "returncode", 0) != 0:
        stderr = (getattr(result, "stderr", "") or "").strip()
        logger.error(f"ffprobe 解析失败：{filepath}: {stderr}")
        return None

    # 解析 JSON。通常第一个视频流在 streams[0]，也可能有音频流排在前面，需要过滤。
    try:
        data = json.loads(result.stdout or "")
    except json.JSONDecodeError:
        logger.error(f"ffprobe JSON 解析失败：{filepath}")
        return None

    streams = data.get("streams") or []
    video_stream = next(
        (stream for stream in streams if isinstance(stream, dict) and stream.get("codec_type") == "video"),
        None,
    )
    if not video_stream:
        logger.error(f"未在文件 {filepath} 中检测到视频流")
        return None

    # 分辨率，优先使用视频流的存储宽高。
    try:
        width = int(video_stream.get("width") or 0)
        height = int(video_stream.get("height") or 0)
    except (TypeError, ValueError):
        logger.error(f"视频分辨率无法解析：{filepath}")
        return None
    if width <= 0 or height <= 0:
        logger.error(f"视频分辨率缺失：{filepath}")
        return None
    file_info["resolution"] = f"{width}x{height}"
    file_info["quality"] = classify_resolution_by_pixels(f"{width}x{height}")

    # 实际宽高比，优先使用显示宽高比；异常时回退到存储宽高比。
    file_info["dar"] = width / height
    dar_str = video_stream.get("display_aspect_ratio")
    if dar_str and dar_str != "N/A":
        try:
            display_width, display_height = map(int, str(dar_str).split(":", maxsplit=1))
            if display_width > 0 and display_height > 0:
                file_info["dar"] = display_width / display_height
            else:
                logger.warning(f"显示宽高比无效，使用存储宽高比：{filepath} {dar_str}")
        except ValueError:
            logger.warning(f"显示宽高比无法解析，使用存储宽高比：{filepath} {dar_str}")

    # 编码器，mkv 要特别判断；MediaInfo 失败时回退到 ffprobe 字段。
    codec_tag_string = str(video_stream.get("codec_tag_string") or "未知编码器")
    codec_name = str(video_stream.get("codec_name") or "未知编码器")
    try:
        codec_detail = check_video_codec(filepath)
    except Exception as e:
        logger.warning(f"MediaInfo 编码解析失败，回退到 ffprobe：{filepath}: {e}")
        codec_detail = None
    if codec_detail and codec_detail not in IGNORED_CODEC_DETAILS and not any(x in codec_detail for x in IGNORED_CODEC_DETAIL_SUBSTRINGS):
        file_info["codec"] = codec_detail
    elif codec_tag_string.startswith("["):
        file_info["codec"] = codec_name
    else:
        file_info["codec"] = codec_tag_string

    # 修剪和规范化编码器名称。
    file_info["codec"] = CODEC_ALIASES.get(file_info["codec"].lower(), file_info["codec"])
    file_info["codec"] = file_info["codec"][:49]
    file_info["codec"] = file_info["codec"].replace("x264pro - Adobe CS Exporter Plug-in", "x264")

    # 比特率，mkv 获取不到视频流码率时，改为获取容器总比特率。
    format_data = data.get("format") or {}
    if not isinstance(format_data, dict):
        format_data = {}
    bit_rate_bps = video_stream.get("bit_rate")
    if not bit_rate_bps:
        bit_rate_bps = format_data.get("bit_rate")
    if bit_rate_bps is None:
        logger.error(f"视频码率缺失：{filepath}")
        return None
    try:
        bit_rate_kbps = int(bit_rate_bps) // 1000
    except (TypeError, ValueError):
        logger.error(f"视频码率无法解析：{filepath} {bit_rate_bps}")
        return None
    file_info["bitrate"] = f"{bit_rate_kbps}kbps"

    # 视频时长，优先使用视频流时长，缺失时使用容器总时长。
    duration = video_stream.get("duration")
    if not duration:
        duration = format_data.get("duration")
    if duration is None:
        logger.error(f"视频时长缺失：{filepath}")
        return None
    try:
        file_info["duration"] = round(float(duration) / 60)
    except (TypeError, ValueError):
        logger.error(f"视频时长无法解析：{filepath} {duration}")
        return None

    # 视频来源，先根据视频文件名判断；找不到再回退到完整路径。
    file_info["source"] = "未知"
    for source_text in (filename, filepath):
        for source in SOURCE_LIST:
            pattern = rf"(?i)(?<![A-Za-z]){source}(?![A-Za-z])"
            if re.search(pattern, source_text):
                file_info["source"] = source
                break
        if file_info["source"] != "未知":
            break

    # 额外处理常见来源写法。
    normalized_filename = filename.lower().replace(".", " ")
    compact_filename = normalized_filename.replace(" ", "").replace("-", "")
    if "blu-ray" in normalized_filename and "remux" in normalized_filename:
        file_info["source"] = "BDRemux"
    elif "bluray" in normalized_filename and "remux" in normalized_filename:
        file_info["source"] = "BDRemux"
    elif "bd" in normalized_filename and "remux" in normalized_filename:
        file_info["source"] = "BDRemux"
    elif "blu-ray" in filename.lower():
        file_info["source"] = "BluRay"
    elif "webdl" in compact_filename:
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


def check_video_codec(path: str | os.PathLike) -> Optional[str]:
    """
    使用 MediaInfo 读取视频编码器和编码参数。

    函数会解析 MediaInfo JSON 输出中的第一个 Video track，
    优先使用 ``Encoded_Library_Name`` 作为编码器名称；若编码设置中包含
    ``crf`` 或 ``rc``，则追加为 ``codec.crf18``、``codec.vbr`` 等形式。
    无法读取媒体信息或找不到视频流时返回 ``None``。

    :param path: 视频文件路径
    :return: 编码器描述；无法解析时返回 ``None``
    """
    path = os.fspath(path)
    # 调用 MediaInfo CLI 获取 JSON 元数据
    cmd = [MEDIAINFO_PATH, '--Output=JSON', path]
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=MEDIAINFO_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        logger.warning(f'Mediainfo 执行超时: {path}')
        return None
    except OSError as e:
        logger.warning(f'Mediainfo 执行失败: {path}: {e}')
        return None

    if getattr(proc, "returncode", 0) != 0:
        stderr = (getattr(proc, "stderr", "") or "").strip()
        logger.warning(f'Mediainfo 执行失败: {path}: {stderr}')
        return None

    try:
        data = json.loads(proc.stdout or "")
    except json.JSONDecodeError:
        logger.warning(f'Mediainfo JSON 解析失败: {path}')
        return None
    if not isinstance(data, dict):
        logger.warning(f'Mediainfo JSON 结构错误: {path}')
        return None

    # 提取 Video track
    media = data.get('media', {})
    if not media:
        logger.warning(f'Mediainfo 解析 JSON 失败: {path}')
        return None

    tracks = media.get('track', [])
    if isinstance(tracks, dict):
        tracks = [tracks]
    if not isinstance(tracks, list):
        logger.warning(f'Mediainfo track 结构错误: {path}')
        return None
    video = next((t for t in tracks if isinstance(t, dict) and t.get('@type') == 'Video'), None)
    if not video:
        logger.warning(f'Mediainfo 未找到视频流: {path}')
        return None

    # 编码器识别
    codec = str(video.get('Encoded_Library_Name') or '').strip()
    if not codec:
        codec = str(video.get('Format') or '').strip().lower()
    if not codec:
        codec = 'Unknown'

    # 解析编码设置: rc_mode / crf / bitrate
    enc_settings = video.get('Encoded_Library_Settings') or video.get('Encoded_Application', '')
    raw_rc = None
    crf_value = None
    target_bitrate = None  # kbps
    if enc_settings:
        for match in re.finditer(r"(rc|crf)=([\w.]+)", enc_settings, flags=re.IGNORECASE):
            key, val = match.groups()
            key = key.lower()
            if key == 'rc':
                raw_rc = val
            elif key == 'crf':
                try:
                    crf_value = int(float(val))
                except ValueError:
                    pass
        m = re.search(r"bitrate=(\d+)", enc_settings, flags=re.IGNORECASE)
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


def generate_video_contact_mtn(video_path: str | os.PathLike) -> None:
    """
    调用 mtn 为视频生成同名 ``_s.jpg`` 网格缩略图。

    该函数作为 ``generate_video_contact`` 的兜底方案使用，输出文件由 mtn
    按视频路径自动生成，调用方仍负责检查 ``_s.jpg`` 是否实际存在。

    :param video_path: 视频文件路径
    :return: 无返回值
    """
    video_path = os.fspath(video_path)
    output_path = os.path.splitext(video_path)[0] + "_s.jpg"
    cmd = [
        MTN_PATH,
        "-c", str(VIDEO_CONTACT_COLUMNS),
        "-r", str(VIDEO_CONTACT_ROWS),
        "-h", str(MTN_CONTACT_THUMB_HEIGHT),
        "-P",
        video_path,
    ]

    logger.info(f"执行 mtn 命令：{subprocess.list2cmdline(cmd)}")
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=MTN_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        logger.warning(f"mtn 执行超时: {video_path}")
        return
    except OSError as e:
        logger.warning(f"mtn 执行失败: {video_path}: {e}")
        return

    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        logger.warning(f"mtn 执行失败: {video_path}: {stderr}")
        return

    if not os.path.exists(output_path):
        logger.warning(f"mtn 未生成缩略图: {output_path}")


def format_video_contact_timestamp(seconds: float) -> str:
    """
    将视频秒数格式化为截图时间戳。

    :param seconds: 视频时间点，单位秒
    :return: ``M:SS.xx`` 或 ``H:MM:SS.xx`` 格式时间戳
    """
    total_centiseconds = max(0, int(round(seconds * 100)))
    total_seconds, centiseconds = divmod(total_centiseconds, 100)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}.{centiseconds:02d}"
    return f"{minutes}:{seconds:02d}.{centiseconds:02d}"


def load_video_contact_timestamp_font(font_size: int) -> Any:
    """
    加载截图时间戳字体，失败时回退到 Pillow 默认字体。

    :param font_size: 字号
    :return: Pillow 字体对象
    """
    fonts_dir = Path(os.environ.get("WINDIR", r"C:\Windows")) / "Fonts"
    for font_name in VIDEO_CONTACT_TIMESTAMP_FONT_NAMES:
        try:
            return ImageFont.truetype(str(fonts_dir / font_name), font_size)
        except OSError:
            pass
    return ImageFont.load_default()


def get_video_contact_text_bbox(draw: Any, text: str, font: Any, stroke_width: int = 0) -> tuple[int, int, int, int]:
    """
    兼容不同 Pillow 版本获取文字边界。

    :param draw: Pillow 绘图对象
    :param text: 待测量文字
    :param font: Pillow 字体对象
    :param stroke_width: 文字描边宽度
    :return: ``(左, 上, 右, 下)``
    """
    try:
        return draw.textbbox((0, 0), text, font=font, stroke_width=stroke_width)
    except TypeError:
        return draw.textbbox((0, 0), text, font=font)
    except AttributeError:
        width, height = draw.textsize(text, font=font)
        return 0, 0, width, height


def get_video_contact_stream_metadata(video_path: str | os.PathLike) -> dict:
    """
    读取首个视频流的 ffprobe 元数据。

    :param video_path: 视频文件路径
    :return: 首个视频流元数据；读取失败时返回空字典
    """
    video_path = os.fspath(video_path)
    cmd = [
        FFPROBE_PATH,
        "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        "-select_streams", "v:0",
        video_path,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=FFPROBE_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        logger.warning(f"ffprobe 读取视频流元数据超时：{video_path}")
        return {}
    except OSError as e:
        logger.warning(f"ffprobe 读取视频流元数据失败：{video_path}: {e}")
        return {}

    if getattr(result, "returncode", 0) != 0:
        stderr = (getattr(result, "stderr", "") or "").strip()
        logger.warning(f"ffprobe 读取视频流元数据失败：{video_path}: {stderr}")
        return {}

    try:
        data = json.loads(result.stdout or "")
    except json.JSONDecodeError:
        logger.warning(f"ffprobe 视频流元数据 JSON 解析失败：{video_path}")
        return {}

    streams = data.get("streams") or []
    stream = next(
        (item for item in streams if isinstance(item, dict) and item.get("codec_type") in (None, "video")),
        None,
    )
    return stream or {}


def is_hdr_video(video_path: str | os.PathLike) -> bool:
    """
    根据视频流色彩元数据判断是否需要 HDR 到 SDR 转换。

    :param video_path: 视频文件路径
    :return: 检测到 HDR/PQ/HLG/BT.2020 元数据时返回 ``True``
    """
    stream = get_video_contact_stream_metadata(video_path)
    color_transfer = str(stream.get("color_transfer") or "").strip().lower()
    color_primaries = str(stream.get("color_primaries") or "").strip().lower()
    side_data_list = stream.get("side_data_list") or []

    if color_transfer in VIDEO_CONTACT_HDR_TRANSFERS:
        return True
    if color_primaries in VIDEO_CONTACT_HDR_PRIMARIES and color_transfer not in VIDEO_CONTACT_SDR_TRANSFERS:
        return True
    if isinstance(side_data_list, list):
        for item in side_data_list:
            side_data_type = str(item.get("side_data_type") if isinstance(item, dict) else "").lower()
            if "mastering display" in side_data_type or "content light" in side_data_type:
                return True
    return False


def extract_video_contact_hdr_frame(video_path: str | os.PathLike, seconds: float) -> Any:
    """
    使用 ffmpeg 将 HDR 单帧 tone-map 为 SDR Pillow 图像。

    :param video_path: 视频文件路径
    :param seconds: 抽帧时间点，单位秒
    :return: 已转换为 RGB 的 Pillow 图像
    """
    video_path = os.fspath(video_path)
    cmd = [
        FFMPEG_PATH,
        "-v", "error",
        "-ss", f"{seconds:.3f}",
        "-i", video_path,
        "-frames:v", "1",
        "-vf", VIDEO_CONTACT_HDR_TONEMAP_FILTER,
        "-f", "image2pipe",
        "-vcodec", "png",
        "-",
    ]
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=FFMPEG_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"ffmpeg HDR 抽帧超时: {seconds:.3f}s") from e
    except OSError as e:
        raise RuntimeError(f"ffmpeg HDR 抽帧启动失败: {e}") from e

    if result.returncode != 0 or not result.stdout:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"ffmpeg HDR 抽帧失败: {seconds:.3f}s {stderr}")

    try:
        return Image.open(io.BytesIO(result.stdout)).convert("RGB")
    except Exception as e:
        raise RuntimeError(f"ffmpeg HDR 抽帧图像解析失败: {seconds:.3f}s") from e


def extract_video_contact_clip_frame(clip: Any, seconds: float) -> Any:
    """
    使用 MoviePy 从普通 SDR 视频抽取单帧。

    :param clip: ``VideoFileClip`` 对象
    :param seconds: 抽帧时间点，单位秒
    :return: Pillow 图像
    """
    with open(os.devnull, "w", encoding="utf-8", errors="ignore") as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
        raw_frame = clip.get_frame(seconds)
    return Image.fromarray(raw_frame.astype('uint8'))


def draw_video_contact_timestamp(frame: Any, seconds: float, font: Any, font_size: int) -> Any:
    """
    在单帧右下角绘制黑底白字时间戳。

    :param frame: Pillow 图像对象
    :param seconds: 当前帧对应的视频秒数
    :param font: Pillow 字体对象
    :param font_size: 字号
    :return: 已绘制时间戳的图像对象
    """
    frame_width, frame_height = frame.size
    timestamp = format_video_contact_timestamp(seconds)
    draw = ImageDraw.Draw(frame, "RGBA")
    padding_x = max(6, font_size // 6)
    padding_y = max(3, font_size // 10)
    margin_x = max(12, frame_width // 25)
    margin_y = max(12, frame_height // 16)
    stroke_width = max(1, font_size // 16)
    text_left, text_top, text_right, text_bottom = get_video_contact_text_bbox(draw, timestamp, font, stroke_width)
    text_width = text_right - text_left
    text_height = text_bottom - text_top
    x2 = max(0, frame_width - margin_x)
    y2 = max(0, frame_height - margin_y)
    x1 = max(0, x2 - text_width - padding_x * 2)
    y1 = max(0, y2 - text_height - padding_y * 2)
    draw.rectangle((x1, y1, x2, y2), fill=VIDEO_CONTACT_TIMESTAMP_BG_COLOR)
    draw.text(
        (x1 + padding_x - text_left, y1 + padding_y - text_top),
        timestamp,
        font=font,
        fill=VIDEO_CONTACT_TIMESTAMP_TEXT_COLOR,
        stroke_width=stroke_width,
        stroke_fill=VIDEO_CONTACT_TIMESTAMP_STROKE_COLOR,
    )
    return frame


def generate_video_contact(video_path: str | os.PathLike) -> None:
    """
    从视频中均匀抽取 16 帧，按 4x4 生成带时间戳的同名 ``_s.jpg`` 网格缩略图。

    函数优先使用 ``extract_video_info`` 解析出的 DAR 修正截图显示宽高比；
    如果 DAR 不可用，则退回 ``VideoFileClip.aspect_ratio``。HDR 视频会使用
    ffmpeg tone mapping 抽帧后再拼图，避免截图发灰。调用方负责在失败时使用
    mtn 兜底，并检查输出文件是否实际存在。

    :param video_path: 视频文件路径
    :return: 无返回值
    """
    video_path = os.fspath(video_path)
    logger.info(f"生成缩略图 {os.path.basename(video_path)}")
    output_path = os.path.splitext(video_path)[0] + "_s.jpg"
    clip = None
    try:
        # clip = VideoFileClip(video_path)
        with open(os.devnull, "w", encoding="utf-8", errors="ignore") as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                clip = VideoFileClip(video_path)

        # 只需要视频存储高度；宽度根据 DAR 手动计算，避免截图被拉伸。
        try:
            _storage_width, storage_height = clip.size
            storage_height = int(storage_height)
        except (TypeError, ValueError) as e:
            raise ValueError(f"视频尺寸无效: {clip.size}") from e
        if storage_height <= 0:
            raise ValueError(f"视频高度无效: {storage_height}")

        try:
            file_info = extract_video_info(video_path) or {}
        except Exception as e:
            logger.warning(f"{video_path} 获取视频 DAR 失败: {e}")
            file_info = {}

        dar = file_info.get("dar") or getattr(clip, "aspect_ratio", 0)
        try:
            dar = float(dar)
        except (TypeError, ValueError) as e:
            raise ValueError(f"视频 DAR 无效: {dar}") from e
        if dar <= 0:
            raise ValueError(f"视频 DAR 无效: {dar}")

        # 根据 DAR 手动计算显示宽度
        display_width = int(storage_height * dar)
        display_height = int(storage_height)
        if display_width <= 0 or display_height <= 0:
            raise ValueError(f"缩略图尺寸无效: {display_width}x{display_height}")

        try:
            duration = float(clip.duration)
        except (TypeError, ValueError) as e:
            raise ValueError(f"视频时长无效: {clip.duration}") from e
        if duration <= 0:
            raise ValueError(f"视频时长无效: {clip.duration}")

        cols, rows = VIDEO_CONTACT_COLUMNS, VIDEO_CONTACT_ROWS
        total_images = cols * rows
        times = [duration * (i + 1) / (total_images + 1) for i in range(total_images)]
        timestamp_font_size = max(VIDEO_CONTACT_TIMESTAMP_MIN_FONT_SIZE, display_height // VIDEO_CONTACT_TIMESTAMP_FONT_DIVISOR)
        timestamp_font = load_video_contact_timestamp_font(timestamp_font_size)
        hdr_video = is_hdr_video(video_path)

        images = []
        for t in times:
            if hdr_video:
                try:
                    frame = extract_video_contact_hdr_frame(video_path, t)
                except RuntimeError as e:
                    logger.warning(f"HDR tone mapping 抽帧失败，退回 MoviePy：{video_path}: {e}")
                    hdr_video = False
                    frame = extract_video_contact_clip_frame(clip, t)
            else:
                frame = extract_video_contact_clip_frame(clip, t)
            # 缩放到正确的显示尺寸（DAR）
            frame = frame.resize((display_width, display_height), Image.Resampling.LANCZOS)
            frame = draw_video_contact_timestamp(frame, t, timestamp_font, timestamp_font_size)
            images.append(frame)

        grid_image = Image.new('RGB', (cols * display_width, rows * display_height))
        for idx, img in enumerate(images):
            col, row = idx % cols, idx // cols
            grid_image.paste(img, (col * display_width, row * display_height))

        grid_image.save(output_path)
        if not os.path.exists(output_path):
            logger.warning(f"未生成视频缩略图: {output_path}")
    finally:
        if clip is not None:
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
