"""
抓取 yts 站点发布。将结果储存到 json 文件中

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import concurrent.futures
import logging
import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict

import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from retrying import retry

from my_module import read_json_to_dict, sanitize_filename, write_dict_to_json, read_file_to_list
from sort_movie_ops import extract_imdb_id, select_best_yts_magnet
from sort_movie_mysql import query_imdb_title_directors
from sort_movie_request import get_tmdb_movie_details

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG = read_json_to_dict('config/scrapy_yts.json')  # 配置文件

YTS_URL = CONFIG['yts_url']  # yts 地址
YTS_USER = CONFIG['yts_user']  # yts 用户
YTS_PASS = CONFIG['yts_pass']  # yts 密码
THREAD_NUMBER = CONFIG['thread_number']  # 线程数
API_PATH = CONFIG['api_path']  # api 请求地址
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录
HEADERS = CONFIG['headers']  # 请求头
COOKIE = CONFIG['cookie']  # 用户甜甜
REQUEST_TIMEOUT = 15
MISS_DIRECTOR_NAME = "_"
NO_DIRECTOR_NAME = "没有导演"

HEADERS["cookie"] = COOKIE  # 请求头加入认证


def scrapy_yts(url_path: str) -> None:
    """
    多线程爬取
    API 地址：https://yts.mx/api/v2/movie_details.json?movie_id=65882

    :param url_path: 储存链接的文件地址
    :return: 无
    """
    # 初始化变量
    failed_list = []
    need_fix_imdb = False

    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=THREAD_NUMBER, pool_maxsize=THREAD_NUMBER)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # 登录yts
    if not yts_login(session):
        return

    # 获取url列表，多线程爬取
    logger.info("开始爬取")
    links = read_file_to_list(url_path)
    try:
        with ThreadPoolExecutor(max_workers=THREAD_NUMBER) as executor:
            futures = {executor.submit(fetch_data, session, link): link for link in links}
            for future in concurrent.futures.as_completed(futures):
                link = futures[future]
                try:
                    result = future.result()
                    if not result:
                        failed_list.append(link)
                    else:
                        handle_result(result, link)
                        if result['data']['movie'].get('director') == MISS_DIRECTOR_NAME:
                            need_fix_imdb = True
                except Exception:
                    failed_list.append(link)
                    logger.exception(f"链接：{link} 在处理进程中发生错误")
    except Exception:
        logger.exception(f"来源文件：{url_path} 在线程分配阶段发生错误")
    finally:
        logger.warning(f"总计数量：{len(links)}，失败数量：{len(failed_list)}。失败链接：")
        for i in failed_list:
            logger.error(i)

        if need_fix_imdb:
            logger.info("来自 yts 没有导演的种子，试图自行补全")
            try:
                scrapy_yts_fix_imdb()
            except Exception:
                logger.exception("yts: 自动补导演时发生错误")


def yts_login(session: requests.Session) -> bool:
    """
    使用给定的 requests.Session 进行登录。

    :param session: 会话
    :return: 返回登录成功或失败的状态
    """
    login_endpoint = f"{YTS_URL}/ajax/login"
    data = {
        "username": YTS_USER,
        "password": YTS_PASS
    }
    try:
        response = session.post(login_endpoint, headers=HEADERS, data=data, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.error(f"yts: 登录请求失败：{exc}")
        return False
    except ValueError:
        logger.error(f"yts: 登录返回了非 JSON 响应")
        return False

    # 登录成功时，返回内容通常为 "Ok."
    if payload.get("status") == "ok":
        logger.info("yts: 登录成功。")
        return True

    logger.error(
        f"yts: 登录失败，status={response.status_code}，响应片段：{response.text[:200]!r}"
    )
    return False


def parse_yts_movie_page(html: str, link: str) -> tuple[str | None, str]:
    """
    解析 YTS 电影详情页，提取电影 ID 和导演名。

    :param html: 页面 HTML
    :param link: 原始链接，用于日志
    :return: (电影 ID, 导演名)
    """
    tree = etree.HTML(html)
    movie_id = tree.xpath('//div[@id="movie-info"]/@data-movie-id')
    if not movie_id:
        logger.error(f"链接：{link} 没有找到电影 ID")
        return None, ""

    director_name = tree.xpath('//span[@itemprop="director"]/span[@itemprop="name"]/text()')
    if not director_name:
        logger.warning(f"链接：{link} 没有找到导演")
        return movie_id[0], MISS_DIRECTOR_NAME

    return movie_id[0], director_name[0]


@retry(stop_max_attempt_number=3, wait_random_min=100, wait_random_max=1200)
def fetch_movie_detail_by_id(session: requests.Session, movie_id: str, link: str) -> Dict:
    """
    根据电影 ID 请求 YTS API 详情。

    :param session: 已登录的 requests.Session
    :param movie_id: 电影 ID
    :param link: 原始链接，用于日志
    :return: 电影详情 JSON；查不到时返回空字典
    """
    api_url = f"{API_PATH}{movie_id}"
    response = session.get(api_url, headers=HEADERS, verify=False, timeout=REQUEST_TIMEOUT)
    movie_detail = response.json()
    if not movie_detail['data']['movie']['id']:
        logger.error(f"链接：{link} 没有返回有效 JSON 数据")
        return {}

    return movie_detail


@retry(stop_max_attempt_number=3, wait_random_min=100, wait_random_max=1200)
def fetch_data(session: requests.Session, link: str) -> Dict:
    """
    获取 json 数据

    :param session: 已登录的 requests.Session
    :param link: 链接
    :return: json 返回数据
    """
    # 访问网页，获取电影 ID
    r2 = session.get(link, headers=HEADERS, verify=False, timeout=REQUEST_TIMEOUT)
    movie_id, director_name = parse_yts_movie_page(r2.text, link)
    if not movie_id:
        return {}

    movie_detail = fetch_movie_detail_by_id(session, movie_id, link)
    if not movie_detail:
        return {}

    movie_detail['data']['movie']['director'] = director_name
    return movie_detail


def build_result_file_name(result: Dict) -> str:
    """
    根据抓取结果生成输出文件名。

    :param result: 请求返回结果
    :return: 清洗后的文件名
    """
    movie = result['data']['movie']
    title = movie['title']
    year = movie['year']
    imdb = "{" + movie['imdb_code'] + "}"
    quality = get_best_quality(result)
    new_file_name = f'{title}({year})[{quality}]{imdb}.json'
    return sanitize_filename(new_file_name)


def build_result_output_path(result: Dict) -> str:
    """
    根据抓取结果生成输出路径。

    :param result: 请求返回结果
    :return: 输出路径
    """
    movie = result['data']['movie']
    director = normalize_director_folder_name(movie['director'])
    file_name = build_result_file_name(result)
    return os.path.join(OUTPUT_DIR, director, file_name)


def handle_result(result: Dict, link: str) -> None:
    """
    将数据写入到本地 JSON 文件

    :param result: 请求返回结果
    :param link: 链接
    :return: 无
    """
    file_path = build_result_output_path(result)
    write_dict_to_json(file_path, result)
    logger.info(f"完成：{link}")


def get_best_quality(result: Dict) -> str:
    """
    从单个 JSON 文件中解析出 ['data']['movie']['torrents']
    列表中各 torrent 的 quality 信息，并返回其中最好的质量
    """
    # 获取 torrents 列表
    torrents = result.get('data', {}).get('movie', {}).get('torrents', [])

    best_quality = ""
    best_value = 0

    for torrent in torrents:
        quality = torrent.get('quality', '')
        # 假设 quality 格式为 '数字p' 如 '720p'、'1080p'
        if quality.endswith('p'):
            try:
                value = int(quality[:-1])
            except ValueError:
                continue
            if value > best_value:
                best_value = value
                best_quality = quality
    return best_quality


def search_tmdb_director(movie_id: str) -> str:
    """
    使用 IMDb 标识查询 TMDb，返回第一个导演名。

    :param movie_id: IMDb 标识，例如 tt1234567
    :return: 导演名；查不到时返回空字符串
    """
    movie_details = get_tmdb_movie_details(movie_id)
    if not movie_details:
        return ""

    crew_list = movie_details['casts'].get('crew', [])
    for member in crew_list:
        if member.get('job') == 'Director':
            return member.get('name') or ""

    return ""


def normalize_director_folder_name(folder_name: str) -> str:
    """
    规范导演目录名。

    :param folder_name: 原始目录名
    :return: 清洗后的目录名
    """
    return folder_name.strip().replace("\"", "")


def resolve_director_name(movie_id: str) -> str:
    """
    按既定顺序解析导演名。

    :param movie_id: IMDb 标识，例如 tt1234567
    :return: 导演目录名
    """
    folder_name = search_imdb_local(movie_id)
    if folder_name:
        return folder_name

    folder_name = search_tmdb_director(movie_id)
    if folder_name:
        return folder_name

    return NO_DIRECTOR_NAME


def move_file_to_director_folder(file_path: Path, target_root: Path, folder_name: str) -> Path:
    """
    将文件移动到导演目录。

    :param file_path: 源文件路径
    :param target_root: 目标根目录
    :param folder_name: 导演目录名
    :return: 目标文件路径
    """
    folder_path = target_root / folder_name
    folder_path.mkdir(parents=True, exist_ok=True)
    target_file_path = folder_path / file_path.name
    shutil.move(file_path, target_file_path)
    return target_file_path


def process_missing_director_file(file_path: Path, target_root: Path) -> None:
    """
    处理单个缺少导演信息的 JSON 文件。

    :param file_path: 源文件路径
    :param target_root: 导演目录的目标根目录
    :return: 无
    """
    file_name = file_path.name
    logger.info(f"处理：{file_name}")

    imdb = extract_imdb_id(file_name)
    if not imdb:
        logger.error(f"没有找到 tt 编号：{file_name}")
        return

    folder_name = resolve_director_name(imdb)
    folder_name = normalize_director_folder_name(folder_name)
    logger.info(f"导演名：{folder_name}")

    move_file_to_director_folder(file_path, target_root, folder_name)
    logger.info("*" * 255)


def scrapy_yts_fix_imdb(miss_path: str = os.path.join(OUTPUT_DIR, MISS_DIRECTOR_NAME)) -> None:
    """
    去 IMDB 获取导演信息，并整理文件

    :return: 无
    """
    # 根据文件名获取 tt 编号
    for root, dirs, files in os.walk(miss_path):
        for file_name in files:
            if file_name.endswith('.json'):
                file_path = Path(os.path.join(root, file_name))
                process_missing_director_file(file_path, Path(root).parent)


def search_imdb_local(movie_id: str) -> str:
    """
    查询本地 IMDb 库，返回用于建目录的导演名。
    查不到或查询失败时返回空字符串。

    :param movie_id: imdb 编号，例如 tt1234567
    :return: 导演名
    """
    logger.info(f"查询本地 IMDb：{movie_id}")
    directors = query_imdb_title_directors(movie_id)

    if directors is None:
        return ""

    for director in directors:
        director_name = (director.get("director_name") or "").strip()
        if director_name:
            return director_name

    return ""
