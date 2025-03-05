"""
从三大网站抓取电影信息，发送请求

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import json
import logging
import sys
import urllib.parse
from typing import Optional

import requests
from bs4 import BeautifulSoup
from retrying import retry
from tmdbv3api import TMDb, Movie, TV, Person
from tmdbv3api.as_obj import AsObj

from my_module import read_json_to_dict

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG = read_json_to_dict('config/sort_movie.json')  # 配置文件

TMDB_URL = CONFIG['tmdb_url']  # tmdb api 地址
TMDB_AUTH = CONFIG['tmdb_auth']  # tmdb api auth
TMDB_KEY = CONFIG['tmdb_key']  # tmdb api key
TMDB_HEADERS = {"Authorization": f"Bearer {TMDB_AUTH}", "accept": "application/json"}

IMDB_MOVIE_URL = CONFIG['imdb_movie_url']  # imdb 电影地址
IMDB_PERSON_URL = CONFIG['imdb_person_url']  # imdb 导演地址
IMDB_HEADER = CONFIG['imdb_header']  # imdb 请求头

DOUBAN_HEADER = CONFIG['douban_header']  # 豆瓣请求头
DOUBAN_COOKIE = CONFIG['douban_cookie']  # 豆瓣cookie
DOUBAN_MOVIE_URL = CONFIG['douban_movie_url']  # 豆瓣电影地址
DOUBAN_PERSON_URL = CONFIG['douban_person_url']  # 豆瓣人物地址
DOUBAN_SEARCH_URL = CONFIG['douban_search_url']  # 豆瓣搜索地址
DOUBAN_HEADER['Cookie'] = DOUBAN_COOKIE  # 请求头加入认证


@retry(stop_max_attempt_number=600, wait_random_min=30, wait_random_max=300)
def get_tmdb_search_response(search_id: str) -> Optional[dict]:
    """
    从 IMDB 搜索，返回结果供解析

    :param search_id: 搜索 id
    :return: 成功时返回 JSON 数据
    """
    print(f"搜索 TMDB：{search_id}")
    url = f"{TMDB_URL}/find/{search_id}?external_source=imdb_id"
    r = requests.get(url, timeout=10, verify=False, headers=TMDB_HEADERS)
    if r.status_code != 200:
        print("请求失败:", r.status_code, r.text)
        return None

    return r.json()


@retry(stop_max_attempt_number=600, wait_random_min=30, wait_random_max=300)
def get_tmdb_movie_details(movie_id: str, tv: bool = False) -> AsObj:
    """
    从 TMDB 获取电影信息，返回结果字典

    :param movie_id: 电影 tmdb 编号
    :param tv: 是否是电视剧，默认为否
    :return: 无
    """
    print(f"查询 TMDB：{movie_id}")
    tmdb = TMDb()
    tmdb.api_key = TMDB_KEY
    movie = TV() if tv else Movie()
    return movie.details(movie_id)


@retry(stop_max_attempt_number=600, wait_random_min=30, wait_random_max=300)
def get_tmdb_director_details(director_id: str) -> AsObj:
    """
    从 TMDB 获取导演信息

    :param director_id: 导演 tmdb 编号
    :return: 返回导演信息字典
    """
    tmdb = TMDb()
    tmdb.api_key = TMDB_KEY
    person = Person()
    return person.details(director_id)


@retry(stop_max_attempt_number=600, wait_random_min=30, wait_random_max=300)
def get_imdb_movie_response(movie_id: str) -> Optional[requests.Response]:
    """
    从 IMDB 获取电影信息，返回结果供解析

    :param movie_id: 电影 imdb 编号
    :return: 成功时返回响应
    """
    print(f"查询 IMDB：{movie_id}")
    url = f"{IMDB_MOVIE_URL}/{movie_id}/"
    response = requests.get(url, timeout=15, verify=False, headers=IMDB_HEADER)
    if response.status_code != 200:
        logger.error(f"IMDB 访问失败！状态码：{response.status_code}")
        return
    return response


@retry(stop_max_attempt_number=600, wait_random_min=30, wait_random_max=300)
def get_imdb_director_response(director_id: str) -> Optional[requests.Response]:
    """
    从 IMDB 获取导演信息，返回结果供解析

    :param director_id: 导演 imdb 编号
    :return: 成功时返回响应
    """
    url = f"{IMDB_PERSON_URL}/{director_id}/"
    response = requests.get(url, timeout=15, verify=False, headers=IMDB_HEADER)
    if response.status_code != 200:
        logger.error(f"IMDB 访问失败！状态码：{response.status_code}")
        return
    return response


def get_imdb_movie_details(movie_id) -> Optional[dict]:
    """
    解析 IMDB 页面的 json 数据

    :param movie_id: 电影 imdb 编号
    :return: 成功时返回解析出来的 json
    """
    response = get_imdb_movie_response(movie_id)
    if not response:
        return

    # 先找到 json 字段，找到 id 为 __NEXT_DATA__ 的 script 标签
    soup = BeautifulSoup(response.text, 'html.parser')
    script_tag = soup.find("script", id="__NEXT_DATA__")
    if script_tag:
        # 提取标签内部的文本内容，并去除首尾可能存在的空白字符
        json_text = script_tag.string.strip()
        try:
            # 将字符串解析为 JSON 对象
            data = json.loads(json_text)
            return data
        except json.JSONDecodeError as e:
            logger.error("IMDB JSON 解析失败:", e)
            return
    else:
        logger.error("IMDB 页面未找到 id 为 __NEXT_DATA__ 的 script 标签")
        return


@retry(stop_max_attempt_number=600, wait_random_min=300, wait_random_max=3000)
def get_douban_movie_response(movie_id: str) -> Optional[requests.Response]:
    """
    从 DOUBAN 获取电影信息，返回结果供解析

    :param movie_id: 电影 douban 编号
    :return: 成功时返回响应
    """
    print(f"查询 DOUBAN：{movie_id}")
    url = f"{DOUBAN_MOVIE_URL}/{movie_id}/"
    response = requests.get(url, timeout=10, verify=False, headers=DOUBAN_HEADER)
    if response.status_code == 403:
        sys.exit(f"豆瓣电影搜索失败，豆瓣拒绝访问，状态码：{response.status_code}")
    elif response.status_code != 200:
        logger.error(f"豆瓣访问失败！状态码：{response.status_code}")
        return None
    return response


@retry(stop_max_attempt_number=600, wait_random_min=300, wait_random_max=3000)
def get_douban_director_response(director_id: str) -> Optional[requests.Response]:
    """
    从 DOUBAN 获取导演信息，返回结果供解析

    :param director_id: 导演 douban 编号
    :return: 成功时返回响应
    """
    url = f"{DOUBAN_PERSON_URL}/{director_id}/"
    response = requests.get(url, timeout=10, verify=False, headers=DOUBAN_HEADER)
    if response.status_code == 403:
        sys.exit(f"豆瓣电影访问失败，豆瓣拒绝访问，状态码：{response.status_code}")
    elif response.status_code != 200:
        logger.error(f"豆瓣访问失败！状态码：{response.status_code}")
        return None
    return response


@retry(stop_max_attempt_number=600, wait_random_min=300, wait_random_max=3000)
def get_douban_search_response(search_id: str, search_type: str) -> Optional[requests.Response]:
    """
    从 DOUBAN 搜索导演信息，返回结果供解析

    :param search_id: 搜索 id
    :param search_type: 搜索类型编码
    :return: 成功时返回响应
    """
    print(f"搜索 DOUBAN：{search_id}")
    url = f"{DOUBAN_SEARCH_URL}?cat={search_type}&q={search_id}"
    response = requests.get(url, timeout=10, verify=False, headers=DOUBAN_HEADER)
    if response.status_code == 403:
        sys.exit(f"豆瓣拒绝访问，状态码：{response.status_code}")
    elif response.status_code != 200:
        logger.error(f"豆瓣访问失败！状态码：{response.status_code}")
        return None
    return response


def get_douban_search_details(r: requests.Response) -> Optional[str]:
    """
    解析 DOUBAN 搜索结果

    :param r: 搜索请求原始响应
    :return: 解析成功时返回 url 结果
    """
    # 解析内容
    soup = BeautifulSoup(r.text, 'html.parser')
    # 定位到 result-list，如果没有任何结果则返回
    result_list = soup.find("div", class_="result-list")
    if not result_list:
        print("豆瓣搜索结果为 0")
        return

    # 获取所有 result 元素，根据搜索结果数量做不同处理
    results = result_list.find_all("div", class_="result")
    count = len(results)
    if count == 1:
        result_div = results[0]
    elif count == 2 and results[0].find("div", class_="pic").find("a").get("title", "").strip() == "It's Hard to be Nice":
        result_div = results[1]
    else:
        print("豆瓣搜索结果过多")
        return

    # 获取链接
    a_tag = result_div.find('a', class_='nbg')
    # 获取 href 属性
    href = a_tag.get('href')
    # 解析 href 获取内部的 URL（需要先解析 query 部分）
    parsed_href = urllib.parse.urlparse(href)
    query_dict = urllib.parse.parse_qs(parsed_href.query)
    # 从 query 中提取 'url' 参数，并解码
    if 'url' in query_dict:
        return urllib.parse.unquote(query_dict['url'][0])
    else:
        logger.error("未找到 url 参数")
        return
