"""
抓取 yts.mx 站点发布。将结果储存到 json 文件中

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import concurrent.futures
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict

import requests
from lxml import etree
from retrying import retry

from my_module import read_json_to_dict, sanitize_filename, write_dict_to_json, read_file_to_list

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG = read_json_to_dict('config/scrapy_yts.json')  # 配置文件

YTS_URL = CONFIG['yts_url']  # yts 地址
YTS_USER = CONFIG['yts_user']  # yts 用户
YTS_PASS = CONFIG['yts_pass']  # yts 密码
THREAD_NUMBER = CONFIG['thread_number']  # 线程数
BASE_URL = CONFIG['base_url']  # 基本地址
API_URL = CONFIG['api_url']  # api 请求地址
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录


def scrapy_yts(url_path: str) -> None:
    """
    多线程爬取
    API 地址：https://yts.mx/api/v2/movie_details.json?movie_id=65882

    :param url_path: 储存链接的文件地址
    :return: 无
    """
    # 初始化变量
    failed = threading.Lock()
    failed_count = 0
    failed_list = []

    # 登录yts
    session = requests.Session()
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
                        with failed:
                            failed_count += 1
                            failed_list.append(link)
                    else:
                        handle_result(result, link)
                except Exception:
                    logger.exception(f"链接：{link} 在处理进程中发生错误")
                    failed_list.append(link)
    except Exception:
        logger.exception(f"链接：{link} 在分配线程时发生错误")
    finally:
        logger.warning(f"总计数量：{len(links)}，失败数量：{failed_count}。失败链接：")
        for i in failed_list:
            logger.error(i)


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
    response = session.post(login_endpoint, data=data)
    # 登录成功时，返回内容通常为 "Ok."
    if response.json()["status"] == "ok":
        logger.info("yts: 登录成功。")
        return True
    else:
        logger.error(f"yts: 登录失败，响应内容: {response.text}")
        return False


@retry(stop_max_attempt_number=2, wait_random_min=100, wait_random_max=1200)
def fetch_data(session: requests.Session, link: str) -> Dict:
    """
    获取 json 数据

    :param session: 已登录的 requests.Session
    :param link: 链接
    :return: json 返回数据
    """
    # 访问网页，获取电影 ID
    r2 = session.get(link, verify=False)
    tree = etree.HTML(r2.text)
    movie_id = tree.xpath('//div[@id="movie-info"]/@data-movie-id')
    if not movie_id:
        logger.error(f"链接：{link} 没有找到电影 ID")
        return {}

    # 获取导演名
    director_name = tree.xpath('//span[@itemprop="director"]/span[@itemprop="name"]/text()')
    if not director_name:
        logger.warning(f"链接：{link} 没有找到导演")
        d_name = "_"
    else:
        d_name = director_name[0]

    # 向 API 发送请求，获取响应，返回最终数据
    api_url = f"{BASE_URL}{API_URL}{movie_id[0]}"
    r1 = session.get(api_url, verify=False)
    movie_detail = r1.json()
    # api 请求的数据有可能滞后，获取不到
    if not movie_detail['data']['movie']['id']:
        logger.error(f"链接：{link} 没有返回有效 JSON 数据")
        return {}
    movie_detail['data']['movie']['director'] = d_name
    return movie_detail


def handle_result(result: Dict, link: str) -> None:
    """
    将数据写入到本地 JSON 文件

    :param result: 请求返回结果
    :param link: 链接
    :return: 无
    """
    director = result['data']['movie']['director']
    title = result['data']['movie']['title']
    year = result['data']['movie']['year']
    imdb = result['data']['movie']['imdb_code']
    imdb = "{" + imdb + "}"
    quality = get_best_quality(result)
    new_file_name = f'{title}({year})[{quality}]{imdb}.json'
    file_name = sanitize_filename(new_file_name)
    file_path = os.path.join(OUTPUT_DIR, director, file_name)
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
