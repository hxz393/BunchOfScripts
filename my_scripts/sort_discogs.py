"""
此模块用于整理下载的各种音乐文件夹，借助 discogs.com 自动整理缺少标签的文件。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import logging
import os
import re
import shutil
from typing import Dict, List, Tuple, Union, Optional, Any

import discogs_client
import mutagen
from retrying import retry

from my_module import remove_readonly_recursive, read_json_to_dict

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict('config/sort_discogs.json')  # 配置文件
UNSUPPORTED_STR = CONFIG['sort_discogs']['unsupported_str']  # 不支持的字符串
VA_LIST = CONFIG['sort_discogs']['va_list']  # 合集标记
USER_TOKEN = CONFIG['sort_discogs']['user_token']  # discogs 个人 token
FAILED_PATH = CONFIG['sort_discogs']['failed_path']  # 失败目录


def sort_audio_file(source_dir: Union[str, os.PathLike]) -> Optional[Tuple[Dict[str, List[str]], List[str]]]:
    """
    对音频文件进行分类，并获取所有音频文件的路径。

    :param source_dir: 音频文件所在的源目录，可以是字符串或 os.PathLike 对象。
    :type source_dir: Union[str, os.PathLike]
    :return: 一个元组，第一个元素是字典，键是文件的扩展名，值是对应扩展名的所有文件的路径列表，
             第二个元素是所有音频文件的路径列表。
    :rtype: Optional[Tuple[Dict[str, List[str]], List[str]]]
    """
    audio_file_dict = {
        'mp3': [],
        'flac': [],
        'wav': [],
        'm4a': [],
        'wv': [],
        'ape': [],
        'ogg': [],
        'wma': [],
    }
    audio_file_list = []

    try:
        if not os.path.exists(source_dir):
            logger.error(f"The directory '{source_dir}' does not exist.")

        if not os.path.isdir(source_dir):
            logger.error(f"'{source_dir}' is not a valid directory.")

        for root, _, files in os.walk(source_dir):
            for file in files:
                file_ext = file.split('.')[-1].lower()
                if file_ext in audio_file_dict:
                    file_path = os.path.normpath(os.path.join(root, file))
                    audio_file_dict[file_ext].append(file_path)
                    audio_file_list.append(file_path)
    except Exception as e:
        logger.error(f"An error occurred while sorting audio files: {e}")

    logger.info(f'获取到文件列表：{audio_file_list}')
    return audio_file_dict, audio_file_list


def get_audio_infos(audio_file_list: List[str]) -> Tuple[List[str], List[str], List[str]]:
    """
    处理各种类型音频文件提取标签信息。

    :param audio_file_list: 音频文件列表。
    :type audio_file_list: List[str]
    :rtype: Tuple[List[str], List[str], List[str]]
    :return: 返回一个元组，包含艺术家名字、专辑名和歌曲名的列表
    """

    artist_list = []
    album_list = []
    title_list = []
    type_list_fine = ['EasyMP4', 'EasyMP3', 'FLAC', 'OggVorbis', 'MonkeysAudio']

    for audio_file in audio_file_list:

        try:
            audio_tags = mutagen.File(audio_file, easy=True)

            if not audio_tags:
                logger.error(f"Failed to read tags from '{audio_file}'.")
                continue

            audio_type = type(audio_tags).__name__
            if audio_type in type_list_fine:
                if 'artist' in audio_tags:
                    artist_list.extend([artist.strip() for artist in audio_tags['artist']])
                elif 'albumartist' in audio_tags:
                    artist_list.extend([artist.strip() for artist in audio_tags['albumartist']])
                album_list.extend([album.strip() for album in audio_tags['album']])
                title_list.extend([title.strip() for title in audio_tags['title']])

            elif audio_type == 'ASF':
                artist_list.append(audio_tags['WM/AlbumArtist'][0].value.strip())
                album_list.append(audio_tags['WM/AlbumTitle'][0].value.strip())
                title_list.append(audio_tags['Title'][0].value.strip())

            elif audio_type == 'WAVE':
                artist_list.append(audio_tags['TPE1'].text[0].strip())
                album_list.append(audio_tags['TALB'].text[0].strip())
                title_list.append(audio_tags['TIT2'].text[0].strip())

        except KeyError:
            pass
        except Exception as e:
            logger.error(f"An error occurred while processing '{audio_file}': {e}")
            continue

    if abs(len(artist_list) - len(audio_file_list)) > 2:
        artist_list.clear()

    title_list = fix_title_list(title_list, audio_file_list)

    logger.info(f'获取到标签信息：{artist_list}, {album_list}, {title_list}')

    return artist_list, album_list, title_list


def fix_string(source_str: str) -> str:
    """
    对输入的字符串进行规则替换。

    本函数定义了一系列正则表达式的模式与替换值，将会对输入的字符串按照这些规则进行处理。处理后的字符串将被用作返回值。

    :param source_str: 输入的字符串，需要被处理的字符串
    :type source_str: str
    :rtype: str
    :return: 经过处理后的字符串
    """
    patterns = [
        (r'^various\sartists|^va\s', 'various'),
        (r'^(a|b|c|d)\d\s-\s|^([abcd])\s\d+\s|^[abcd]\d\.-\s', ''),
        (r'\(\d{4}\)|\[\d{4}\]', ''),
        (r'\(\d\scd\)|\(\dcd\)', ''),
        (r'^\d{1,2}\s|^\d{2}-\s', ''),
        (r'\scd\d|cd\d|cd\s\d|disc\s\d', ''),
        (r'^track\s\d+|^faixa\s\d+|^pista\s\d+', ''),
        (r'^\d+-\d+-', ''),
        (r'_', ' '),
        (r'mp3|flac', ''),
        (r'side\s[ab]', ''),
    ]

    new_str = source_str.lower()

    try:
        for pattern, replacement in patterns:
            new_str = re.sub(pattern, replacement, new_str).strip()
    except Exception as e:
        logger.error(f"An error occurred while fixing string: {e}")

    return new_str


def fix_title_list(title_list: List[str], audio_file_list: List[str]) -> Optional[List[str]]:
    """
    修复标题列表。如果原标题列表与音频文件列表的长度差距小于3，就返回修复后的原标题列表。
    否则，返回从音频文件列表提取并修复后的新标题列表。

    :type title_list: List[str]
    :param title_list: 原标题列表。
    :type audio_file_list: List[str]
    :param audio_file_list: 音频文件列表，包含音频文件的全路径。
    :rtype: Optional[List[str]]
    :return: 修复后的标题列表，或者在出现异常时返回 None。
    """
    try:
        if abs(len(title_list) - len(audio_file_list)) == 0:
            return [fix_string(title) for title in title_list]
        else:
            return [fix_string(os.path.splitext(os.path.basename(audio_file).lower())[0]) for audio_file in audio_file_list]
    except Exception as e:
        logger.error(f"An error occurred while fixing the title list: {e}")
        return None


def sort_discogs(source_path: str, target_path: str, no_query: bool = False) -> Dict[str, str]:
    """
    通过查询 Discogs 音乐库整理本地下载文件夹。

    :type source_path: str
    :param source_path: 需要排序的音乐库的源路径。
    :type target_path: str
    :param target_path: 排序后的音乐库的目标路径。
    :type no_query: bool
    :param no_query: 是否进行联网查询。如果为 True，则不进行联网查询。
    :rtype: Dict[str, str]
    :return: 返回字典，包含源文件和目标文件的路径映射
    """
    source_names = os.listdir(source_path)
    for source_name in source_names:
        logger.info(f"开始处理文件夹：{source_name}")
        source_dir = os.path.join(source_path, source_name)
        target_dir = ''
        remove_readonly_recursive(source_dir)

        # 获取文件列表
        audio_file_dict, audio_file_list = sort_audio_file(source_dir)
        if len(audio_file_list) == 0:
            logger.info(f"没有文件，跳过：{source_dir}")
            logger.info('#' * 166)
            continue

        # 获取标签信息
        artist_list, album_list, title_list = get_audio_infos(audio_file_list)

        # 根据标签信息做不同处理
        if len(set(artist_list)) == 1 and artist_list[0].lower() not in VA_LIST:
            logger.info(f"标签完备，不需要联网查询：{source_name}")
            artist = re.sub(UNSUPPORTED_STR, "_", artist_list[0])
            target_dir = os.path.join(target_path, artist, source_name)
        elif no_query:
            logger.info(f"不联网查询，跳过：{source_dir}")
            logger.info('#' * 166)
            continue
        elif len(set(artist_list)) == 1 and artist_list[0].lower() in VA_LIST:
            logger.info(f"正常合集：{source_name}")
            album = fix_string(album_list[0].lower()) if album_list else source_name
            title1 = title_list[0]
            title2 = title_list[-1]
            search_data = list({f'{title1} {title2}', f'various {album}', f'{album} {title1}', f'{album} {title2}'})
            artist = search_discogs(search_data, title_list)
            target_dir = os.path.join(target_path, artist, source_name) if artist else ''
        elif len(set(artist_list)) > 1 and len(set(album_list)) > 1:
            logger.info(f"多专辑多艺术家：{source_name}")
            top_name = fix_string(source_name.replace("_", " "))
            sub_name = fix_string(os.path.relpath(os.path.dirname(audio_file_list[0]), source_dir).replace("_", " "))
            artist1 = fix_string(artist_list[0].lower())
            artist2 = fix_string(artist_list[-1].lower())
            title1 = title_list[0]
            title2 = title_list[-1]
            search_data = list({f'{title1} {title2}', f'{artist1} {sub_name}', f'{artist2} {top_name}', top_name, f'{top_name} {title1}', f'{sub_name} {title2}'})
            artist = search_discogs(search_data, title_list)
            target_dir = os.path.join(target_path, artist, source_name) if artist else ''
        elif len(set(artist_list)) > 1 and len(set(album_list)) == 1:
            logger.info(f"单专辑多艺术家：{source_name}")
            album = fix_string(album_list[0].lower())
            artist1 = fix_string(artist_list[0].lower())
            artist2 = fix_string(artist_list[-1].lower())
            title1 = title_list[0]
            title2 = title_list[-1]
            search_data = list({f'{title1} {title2}', f'{artist1} {album}', f'{artist2} {album}', album, f'{album} {title1}', f'{album} {title2}'})
            artist = search_discogs(search_data, title_list)
            target_dir = os.path.join(target_path, artist, source_name) if artist else ''
        elif len(set(artist_list)) > 1 and len(set(album_list)) == 0:
            logger.info(f"无专辑多艺术家：{source_name}")
            top_name = fix_string(source_name.replace("_", " "))
            sub_name = fix_string(os.path.relpath(os.path.dirname(audio_file_list[0]), source_dir).replace("_", " "))
            artist1 = fix_string(artist_list[0].lower())
            artist2 = fix_string(artist_list[-1].lower())
            title1 = title_list[0]
            title2 = title_list[-1]
            search_data = list({f'{title1} {title2}', f'{artist1} {sub_name}', f'{artist2} {top_name}', top_name, sub_name, f'{top_name} {title1}', f'{sub_name} {title2}'})
            artist = search_discogs(search_data, title_list)
            target_dir = os.path.join(target_path, artist, source_name) if artist else ''
        elif len(set(artist_list)) == 0 and len(set(album_list)) > 1:
            logger.info(f"多专辑无艺术家：{source_name}")
            album = fix_string(album_list[0].lower())
            top_name = fix_string(source_name.replace("_", " "))
            title1 = title_list[0]
            title2 = title_list[-1]
            search_data = list({f'{title1} {title2}', album, top_name, f'{album} {title1}', f'{album} {title2}'})
            artist = search_discogs(search_data, title_list)
            target_dir = os.path.join(target_path, artist, source_name) if artist else ''
        elif len(set(artist_list)) == 0 and len(set(album_list)) == 1:
            logger.info(f"单专辑无艺术家：{source_name}")
            album = fix_string(album_list[0].lower())
            title1 = title_list[0]
            title2 = title_list[-1]
            search_data = list({f'{title1} {title2}', album, f'{album} {title1}', f'{album} {title2}'})
            artist = search_discogs(search_data, title_list)
            target_dir = os.path.join(target_path, artist, source_name) if artist else ''
        elif len(set(artist_list)) == 0 and len(set(album_list)) == 0:
            logger.info(f"无专辑无艺术家：{source_name}")
            top_name = fix_string(source_name.replace("_", " "))
            sub_name = fix_string(os.path.relpath(os.path.dirname(audio_file_list[0]), source_dir).replace("_", " "))
            title1 = title_list[0]
            title2 = title_list[-1]
            search_data = list({f'{title1} {title2}', top_name, sub_name, f'{top_name} {title1}', f'{top_name} {title2}', f'{sub_name} {title1}', f'{sub_name} {title2}'})
            artist = search_discogs(search_data, title_list)
            target_dir = os.path.join(target_path, artist, source_name) if artist else ''
        else:
            logger.info(f"奇怪的目录：{source_name}")

        if os.path.exists(target_dir):
            logger.warning(f"目标已存在，不移动：{source_dir}")
        elif not target_dir:
            shutil.move(source_dir, FAILED_PATH)
            logger.warning(f"没有结果，目标：{source_dir} 移动到 {FAILED_PATH}")
        else:
            shutil.move(source_dir, target_dir)
            logger.info(f"目标：{source_dir} 移动到 {target_dir}")
        logger.info('#' * 166)


@retry(stop_max_attempt_number=1200, wait_random_min=100, wait_random_max=1200)
def search_discogs(search_data: List[str], title_list: List[str]) -> str:
    """
    在 Discogs 上搜索专辑并筛选。

    :param search_data: 需要搜索的专辑列表。
    :type search_data: List[str]
    :param title_list: 用于筛选的标题列表。
    :type title_list: List[str]
    :rtype: str
    :return: 艺术家名。
    """
    artist = ''
    searched_ids = []
    client = discogs_client.Client('ExampleApplication/0.1', user_token=USER_TOKEN)
    try:
        for data in search_data:
            logger.info(f'开始在discogs上搜索专辑：{data}')
            response = client.search(data, type='release')
            result_len = len(response)
            if response.pages == 1 and result_len == 0:
                logger.info(f'没有搜索结果：{data}')
            elif result_len > 0:
                logger.info(f'找到 {result_len} 个结果')
                artist, searched_ids = filter_response(response, title_list, searched_ids)
                if artist:
                    return artist
                else:
                    logger.info(f'没有匹配结果：{data}')
            else:
                logger.error(f'没见过的错误？？：{data}')
    except Exception as e:
        logger.error(f"搜索错误？？：{e}")
    return artist


# @retry(stop_max_attempt_number=1200, wait_random_min=100, wait_random_max=1200)
def filter_response(response: List, title_list: List[str], searched_ids: List[str]) -> Tuple[Optional[str], List[str]]:
    """
    根据指定的响应，标题列表和已搜索ID列表过滤响应。

    :type response: List
    :param response: 待处理的响应列表。

    :type title_list: List[str]
    :param title_list: 标题列表。

    :type searched_ids: List[str]
    :param searched_ids: 已经搜索过的ID列表。

    :rtype: Tuple[Optional[str], List[str]]
    :return: 一个元组，其中第一项是艺术家的名称，第二项是包含已搜索过的ID的列表。
    """
    artist = None
    result_len = len(response)
    search_limit = get_search_limit(result_len)

    for i in range(search_limit):
        # 检查是否已经查询
        search_id = response[i].id
        if search_id in searched_ids:
            logger.info(f'匹配第 {i + 1} 条记录是重复 ID，跳过')
            continue

        # 检查音轨数目差别
        result_tracklist = response[i].tracklist
        track_count = len(result_tracklist)
        if abs(track_count - len(title_list)) > track_count // 20 + 1:
            logger.info(f'匹配第 {i + 1} 条记录曲目数差别过大，跳过')
            searched_ids.insert(0, search_id)
            continue

        # 计算正确率
        result_artist = response[i].artists[0].name if response[i].artists else ''
        hits, hits_rate = get_hits_rate(track_count, result_tracklist, title_list)

        if hits_rate > 0.9:
            artist = response[i].labels[0].name if result_artist == 'Various' else result_artist
            artist = re.sub(UNSUPPORTED_STR, "_", artist)
            artist = re.sub(r'\s\(\d+\)', "", artist)
            logger.info(f'正确率计数 {hits}，正确率为：{hits_rate}。匹配完成')
            return artist, searched_ids
        else:
            logger.info(f'匹配第 {i + 1} 条记录失败，正确率计数：{hits})，正确率为：{hits_rate}。正确率过低')

        searched_ids.insert(0, search_id)
    return artist, searched_ids


def get_search_limit(result_len: Union[int, float]) -> int:
    """
    根据输入结果的长度计算搜索限制。

    :param result_len: 输入结果的长度。
    :type result_len: Union[int, float]
    :rtype: int
    :return: 搜索限制的值。
    """
    try:
        if not isinstance(result_len, (int, float)):
            logger.error(f"'{result_len}' is not a valid number.")
            return 1

        if result_len > 500:
            search_limit = 1
        elif result_len > 100:
            search_limit = 3
        elif result_len > 30:
            search_limit = 4
        elif result_len > 5:
            search_limit = 5
        else:
            search_limit = result_len

        return int(search_limit)

    except Exception as e:
        logger.error(f"An error occurred while calculating search limit: {e}")
        return 1


def get_hits_rate(track_count: int, result_tracklist: Any, title_list: List[str]) -> Optional[Tuple[int, float]]:
    """
    计算正确率。

    :type track_count: int
    :param track_count: 需要处理的曲目数量。

    :type result_tracklist: List[object]
    :param result_tracklist: 结果曲目列表，列表中每个元素都是一个有 title 属性的对象。

    :type title_list: Any
    :param title_list: 标题列表，每个元素都是一个字符串类型的标题。

    :rtype: Optional[Tuple[int, float]]
    :return: 一个元组，第一个元素是匹配的数量，第二个元素是匹配的比率。如果处理过程中发生错误，返回 0。
    """
    hits = 0
    hits_rate = 0
    try:
        for j in range(track_count):
            result_title = re.sub(r"[/\\:*?\"<>|.()\[\]^$+{}]", " ", fix_string(result_tracklist[j].title.lower())).strip()
            result_hit = len(re.findall(result_title, ', '.join(title_list).lower()))
            if result_hit > 0:
                hits += 1
            else:
                logger.info(f'第{j + 1}首歌标题不匹配，线上曲目标题为：{result_title}')
        hits_rate = hits / track_count
    except Exception as e:
        logger.error(f"计算正确率时发生错误：{e}")
    return hits, hits_rate
