"""
从三大网站抓取电影信息，发送请求

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import base64
import json
import logging
import os.path
import re
import sys
import time
import urllib.parse
from collections import defaultdict
from typing import Optional, cast

import requests
import xmltodict as xmltodict
from bs4 import BeautifulSoup
from retrying import retry
from tmdbv3api import TMDb, Movie, TV, Person
from tmdbv3api.as_obj import AsObj
from tmdbv3api.exceptions import TMDbException

from my_module import read_json_to_dict

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG = read_json_to_dict('config/sort_movie_request.json')  # 配置文件

TMDB_URL = CONFIG['tmdb_url']  # tmdb api 地址
TMDB_AUTH = CONFIG['tmdb_auth']  # tmdb api auth
TMDB_KEY = CONFIG['tmdb_key']  # tmdb api key
TMDB_HEADERS = {"Authorization": f"Bearer {TMDB_AUTH}", "accept": "application/json"}
TMDB_IMAGE_URL = CONFIG['tmdb_image_url']  # tmdb 图片地址

DOUBAN_HEADER = CONFIG['douban_header']  # 豆瓣请求头
DOUBAN_COOKIE = CONFIG['douban_cookie']  # 豆瓣cookie
DOUBAN_MOVIE_URL = CONFIG['douban_movie_url']  # 豆瓣电影地址
DOUBAN_PERSON_URL = CONFIG['douban_person_url']  # 豆瓣人物地址
DOUBAN_SEARCH_URL = CONFIG['douban_search_url']  # 豆瓣搜索地址
DOUBAN_HEADER['Cookie'] = DOUBAN_COOKIE  # 请求头加入认证

CSFD_HEADER = CONFIG['csfd_header']  # csfd 请求头
CSFD_COOKIE = CONFIG['csfd_cookie']  # 豆瓣cookie
CSFD_HEADER['Cookie'] = CSFD_COOKIE  # 请求头加入认证

KPK_SEARCH_URL = CONFIG['kpk_search_url']  # 科普库搜索地址
KPK_PAGE_URL = CONFIG['kpk_page_url']  # 科普库搜索地址
KPK_HEADER = CONFIG['kpk_header']  # 科普库请求头

JACKETT_SEARCH_URL = CONFIG['jackett_search_url']  # jackett 搜索地址
JACKETT_API_KEY = CONFIG['jackett_api_key']  # jackett api 密钥

TMDB = TMDb()
TMDB.api_key = TMDB_KEY


@retry(stop_max_attempt_number=50, wait_random_min=300, wait_random_max=3000)
def get_tmdb_search_response(search_id: str) -> Optional[dict]:
    """
    从 IMDB 搜索，返回结果供解析

    :param search_id: 搜索 id
    :return: 成功时返回 JSON 数据
    """
    logger.info(f"搜索 TMDB：{search_id}")
    url = f"{TMDB_URL}/find/{search_id}?external_source=imdb_id"
    r = requests.get(url, timeout=10, verify=False, headers=TMDB_HEADERS)
    if r.status_code == 403:
        logger.error("TMDB 拒绝访问：状态码 %s", r.status_code)
        sys.exit(f"TMDB 拒绝访问 {r.status_code}：{url}")
    if r.status_code != 200:
        logger.error("TMDB 请求失败：%s %s", r.status_code, r.text)
        raise Exception(f"TMDB 请求失败：{r.status_code}")

    return r.json()


@retry(stop_max_attempt_number=50, wait_random_min=1000, wait_random_max=5000)
def get_tmdb_movie_details(movie_id: str, tv: bool = False) -> Optional[dict]:
    """
    从 TMDB 获取电影信息，返回结果字典

    :param movie_id: 电影 tmdb 编号
    :param tv: 是否是电视剧，默认为否
    :return: 信息字典
    """
    logger.info(f"查询 TMDB：{movie_id}")
    movie = TV() if tv else Movie()

    try:
        result = dict(movie.details(movie_id)) | dict(movie.alternative_titles(movie_id))
        if not result:
            logger.info(f"获取 {movie_id} 电影信息失败，重试")
            raise Exception("从 TMDB 获取电影信息失败")
        return result
    except TMDbException as e:
        # 处理 TMDB API 抛出的特定异常
        error_msg = str(e)
        if "could not be found" in error_msg.lower():
            logger.warning(f"TMDB 没有记录 {movie_id}")
            return None
        # 其他 TMDB 错误交给 retry 处理
        logger.warning(f"TMDB 查询失败 {movie_id}: {error_msg}")
        raise


@retry(stop_max_attempt_number=50, wait_random_min=300, wait_random_max=3000)
def get_tmdb_director_details(director_id: str) -> AsObj:
    """
    从 TMDB 获取导演个人信息

    :param director_id: 导演 tmdb 编号
    :return: 返回导演信息字典
    """
    person = Person()
    return person.details(director_id)


@retry(stop_max_attempt_number=50, wait_random_min=300, wait_random_max=3000)
def get_tmdb_director_movies(director_id: str) -> AsObj:
    """
    从 TMDB 获取导演电影信息

    :param director_id: 导演 tmdb 编号
    :return: 返回导演信息字典
    """
    person = Person()
    return person.movie_credits(director_id)


@retry(stop_max_attempt_number=5, wait_random_min=5330, wait_random_max=15800)
def get_tmdb_movie_cover(poster_path: str, target_path: str) -> None:
    """
    从 TMDB 获取电影海报地址

    :param poster_path: 电影海报地址，半截。例如：/eqMlCJo54tyoEGI9UMxp70Ys7kU.jpg
    :param target_path: 储存路径
    :return: 无
    """
    if not poster_path:
        logger.warning("没封面图地址，请手动下载")
        return

    # 完整图片URL
    image_url = f"{TMDB_IMAGE_URL}{poster_path}"

    # 下载图片
    image_response = requests.get(image_url, timeout=60, verify=False, headers=TMDB_HEADERS)
    if image_response.status_code == 200:
        with open(target_path, 'wb') as f:
            f.write(image_response.content)
        logger.info(f"封面下载成功，保存为 {os.path.basename(target_path)}")
    else:
        logger.error(f"封面下载失败：状态码 {image_response.status_code}")
        raise Exception(f"封面下载失败 {image_url}")


@retry(
    stop_max_attempt_number=5,
    wait_random_min=1000,
    wait_random_max=3000,
    retry_on_exception=lambda e: isinstance(e, requests.RequestException),
)
def get_csfd_response(url: str) -> requests.Response:
    """
    从 CSFD 获取电影信息，返回结果供解析

    :param url: csfd 链接
    :return: 成功时返回响应
    """
    try:
        response = requests.get(
            url,
            timeout=15,
            verify=False,
            allow_redirects=True,
            headers=CSFD_HEADER,
        )
        if response.status_code == 403:
            sys.exit(f"CSFD 拒绝访问，状态码：{response.status_code}，退出程序")
        # 429 和 5xx 视为暂时性错误，交给 retry 重试
        if response.status_code == 429 or 500 <= response.status_code < 600:
            response.raise_for_status()
        # 其他 4xx 一般是链接本身有问题，直接失败，不重试
        if response.status_code >= 400:
            raise RuntimeError(f"CSFD 访问失败：{response.status_code}  url={url}")
        return response

    except requests.exceptions.RequestException as e:
        # 记录错误并重新抛出，让 @retry 生效
        logger.warning(f"请求 CSFD 失败（将重试）: {e}  url={url}")
        raise


def get_csfd_movie_details(r: requests.Response) -> Optional[dict]:
    """
    解析 csfd 搜索结果

    :param r: 搜索请求原始响应
    :return: 解析成功时返回数据字典
    """
    # 解析内容
    soup = BeautifulSoup(r.text, 'html.parser')
    # 1) 国家、年份、时长
    origin_div = soup.find('div', class_='origin')
    parts = [s.rstrip(',') for s in origin_div.stripped_strings]
    origin = " ".join(part for part in parts if part).strip()

    # 2) 导演
    director = ""
    creators = soup.find('div', id='creators')
    # 找到 <h4> 标签里包含 “Režie:” 的那一组，再取它后面的第一个 <a>
    for block in creators.find_all('div'):
        h4 = block.find('h4')
        if not h4:
            continue
        h4_text = h4.get_text()
        if 'Režie' in h4_text or 'Directed' in h4_text:
            a = block.find('a', href=True)
            director = a.get_text(strip=True) if a else ""
            break

    # 3) IMDb 编号（或用 csfd 编号作后备）
    m_id = None
    imdb_tag = soup.find('a', class_='button-imdb', href=True)
    if imdb_tag:
        href = imdb_tag['href']
        m = re.search(r'/title/(tt\d+)', href)
        m_id = m.group(1) if m else None
    else:
        same_as = soup.find('a', itemprop='sameAs', href=True)
        if same_as:
            csfd_id = same_as['href'].rstrip('/').split('/')[-1]
            m_id = f"csfd{csfd_id}"

    # 最终结果
    data = {
        "origin": origin,  # e.g. "Sovětský svaz, 1974, 170 min"
        "director": director,  # e.g. "Igor Gostev"
        "id": m_id  # e.g. "tt0071525" 或 "csfd102778"
    }

    return data


@retry(stop_max_attempt_number=15, wait_random_min=1300, wait_random_max=3000)
def get_douban_response(db_id: str, query_type: str) -> Optional[requests.Response]:
    """
    从 DOUBAN 获取响应，返回结果供解析

    :param db_id: 参数 id
    :param query_type: 请求类型
    :return: 成功时返回响应
    """
    if query_type == "movie_response":
        logger.info(f"查询 DOUBAN：{db_id}")
        url = f"{DOUBAN_MOVIE_URL}/{db_id}/"
    elif query_type == "director_response":
        url = f"{DOUBAN_PERSON_URL}/{db_id}/"
    elif query_type == "director_search":
        logger.info(f"搜索 DOUBAN 导演：{db_id}")
        url = f"{DOUBAN_SEARCH_URL}?cat=1065&q={db_id}"
    elif query_type == "movie_search":
        logger.info(f"搜索 DOUBAN：{db_id}")
        url = f"{DOUBAN_SEARCH_URL}?cat=1002&q={db_id}"
    else:
        raise ValueError(f"未知的豆瓣请求类型：{query_type}")

    response = requests.get(url, timeout=10, verify=False, headers=DOUBAN_HEADER)
    logger.debug(response.text)
    if response.status_code == 403:
        sys.exit(f"豆瓣拒绝访问，状态码：{response.status_code}，退出程序")
    elif response.status_code != 200:
        logger.info(f"豆瓣访问失败！状态码：{response.status_code}，重试！")
        raise Exception(f"豆瓣访问失败！")
    elif response.text.find("登录跳转") != -1:
        sys.exit("豆瓣弹出验证页！")
    return response


def get_douban_search_details(r: requests.Response) -> Optional[str]:
    """
    解析 DOUBAN 搜索结果，返回唯一可信目标的链接。

    这个函数不是通用的“搜索结果列表解析器”，而是保守筛选器：
    只有在结果足够明确时才返回一条目标 URL；结果为 0、结果过多、
    或页面结构不符合预期时返回 ``None``。如果遇到豆瓣验证页，则直接退出。

    :param r: 搜索请求原始响应
    :return: 解析成功时返回唯一目标 URL，无法唯一确定时返回 None
    """
    # 解析内容
    soup = BeautifulSoup(r.text, 'html.parser')
    # 检查是否有搜索框，如果弹验证不会出现这个 div
    result_div = soup.find("div", class_="search-result")
    if not result_div:
        sys.exit("豆瓣弹出验证页！")

    # 定位到 result-list，如果没有任何结果则返回
    result_list = soup.find("div", class_="result-list")
    if not result_list:
        logger.warning("豆瓣搜索结果为 0")
        return

    # 获取所有 result 元素。这里只接受“唯一结果”或“一个已知噪声项 + 一个真实结果”
    # 两种情况，其他多结果场景一律放弃判断，交给上层按未命中处理。
    results = result_list.find_all("div", class_="result")
    count = len(results)
    if count == 1:
        result_div = results[0]
    # 兼容历史上遇到的一个特例：搜索结果会先出现一个固定噪声项
    # “It's Hard to be Nice”，此时真实目标在第二条。
    elif count == 2 and results[0].find("div", class_="pic").find("a").get("title", "").strip() == "It's Hard to be Nice":
        result_div = results[1]
    else:
        logger.warning("豆瓣搜索结果过多")
        return

    # 获取链接
    a_tag = result_div.find('a', class_='nbg')
    # 获取 href 属性
    href = a_tag.get('href')
    if not isinstance(href, str):
        logger.error("未找到有效 href")
        return
    # 解析 href 获取内部的 URL（需要先解析 query 部分）
    parsed_href = urllib.parse.urlparse(href)
    query_dict = cast(dict[str, list[str]], urllib.parse.parse_qs(parsed_href.query))
    # 从 query 中提取 'url' 参数，并解码
    if 'url' in query_dict:
        return urllib.parse.unquote(query_dict['url'][0])
    else:
        logger.error("未找到 url 参数")
        return


@retry(stop_max_attempt_number=50, wait_random_min=300, wait_random_max=6000)
def get_jackett_search_response(search_id: str) -> list:
    """
    从捷克三搜索 imdb 编号，返回响应。只搜索孤品 id

    :param search_id: 搜索 id
    :return: 成功时返回响应
    """
    url = f"{JACKETT_SEARCH_URL}/api/v2.0/indexers/all/results/torznab/api?apikey={JACKETT_API_KEY}&q={search_id}"
    response = requests.get(url, timeout=30, verify=False, allow_redirects=True)
    if response.status_code == 403:
        sys.exit(f"Jackett 拒绝访问，状态码：{response.status_code}，退出程序")
    if not response:
        logger.info(f"Jackett 没有返回数据")
        raise Exception(f"Jackett 访问网络失败！")
    response.encoding = 'utf-8'
    try:
        data_dict = xmltodict.parse(response.text)
        result_rss = data_dict.get('rss', {}).get('channel', {})
        if not result_rss:
            logger.info(f"Jackett 没有返回数据")
            raise Exception(f"Jackett 访问网络失败！")

        result_items = result_rss.get("item")
        if not result_items:
            logger.info(f"Jackett 没有搜索结果")
            return []
        if isinstance(result_items, dict):
            result_items = [result_items]
        return result_items

    except Exception as e:
        logger.info(f"响应解析错误: {e}")
        raise Exception(f"Jackett XML 解析失败！")


@retry(stop_max_attempt_number=50, wait_random_min=1300, wait_random_max=9000)
def get_kpk_search_response(search_id: str) -> Optional[list]:
    """
    在 kpk 搜索 imdb 编号，返回结果页面 id

    :param search_id: 搜索 id
    :return: 成功时返回响应网页 id 列表
    """
    url = f"{KPK_SEARCH_URL}"
    params = {
        "kw": search_id,
        "callback": "jQuery112305981342517550043_1742472456793",
    }
    response = requests.get(url, timeout=10, verify=False, headers=KPK_HEADER, params=params)
    if response.status_code == 403:
        sys.exit(f"科普库拒绝访问，状态码：{response.status_code}，退出程序")
    if response.status_code != 200:
        raise Exception(f"科普库访问失败！状态码：{response.status_code}")
    # 去掉 JSONP 回调函数的包装
    response.encoding = 'utf-8'
    try:
        json_str = re.sub(r'.+({.+}).+', r'\1', response.text)
        data_obj = json.loads(json_str)
    except json.decoder.JSONDecodeError as e:
        logger.info(f"JSON 解析错误: {e}")
        raise  # 抛出异常让retry捕获并重试

    if data_obj["code"] != 1:
        logger.info(f"失败重试: {data_obj}")
        raise Exception("请求被拒绝！")
    # 格式化解码后的 JSON 数据
    result = []
    if data_obj["js"]:
        result = json.loads(base64.b64decode(data_obj["js"]).decode('utf-8'))

    # 返回 id 列表
    if not result:
        return
    return [i["id"] for i in result]


@retry(stop_max_attempt_number=50, wait_random_min=1300, wait_random_max=3000)
def get_kpk_page_details(page_id: str) -> dict:
    """
    访问 kpk 详情页，获取下载信息

    :param page_id: 页面 id
    :return: 成功时返回下载信息字典
    """
    url = f"{KPK_PAGE_URL}/{page_id}"
    response = requests.get(url, timeout=15, verify=False, headers=KPK_HEADER)
    if response.status_code == 403:
        sys.exit(f"科普库拒绝访问，状态码：{response.status_code}，退出程序")
    if response.status_code != 200:
        raise Exception(f"科普库访问失败！状态码：{response.status_code}")

    # 创建一个默认字典来存放结果
    result_dict = defaultdict(list)
    soup = BeautifulSoup(response.text, 'html.parser')
    # 获取所有指定class的h2元素
    for h2_tag in soup.find_all('h2', class_='uk-text-bold uk-text-muted'):
        # 查找当前h2之后的第一个table
        table = h2_tag.find_next('table')
        if table:
            # 遍历table中所有的tr
            for tr in table.find_all('tr'):
                td = tr.find('td')
                if td:
                    # 提取span文本
                    span = td.find('span')
                    # 提取第一个有效的a标签文本
                    a = td.find('a')

                    # 拼接内容
                    full_text = f"{span.get_text(strip=True)} {a.get_text(strip=True)}" if span else a.get_text(strip=True)
                    full_text = full_text.replace('复制链接', '').replace('详情', '').strip()
                    result_dict[h2_tag.get_text(strip=True)].append(full_text)
    return dict(result_dict)


def check_kpk_for_better_quality(imdb: str, quality: str) -> bool:
    """
    通过 IMDB 编号搜索科普库，判断是否存在更高质量版本。

    :param imdb: IMDB 编号
    :param quality: 当前视频质量
    :return: 有更好质量时返回 True
    """
    logger.info(f"搜索科普库：{imdb}")
    ids = get_kpk_search_response(imdb)
    if not ids:
        logger.info(f"科普库没有结果：{imdb}")
        return False
    logger.debug(ids)

    merged_dict = defaultdict(list)
    for page_id in ids:
        result = get_kpk_page_details(page_id)
        if result:
            for key, value in result.items():
                merged_dict[key].extend(value)
    merged_dict = dict(merged_dict)
    if not merged_dict:
        logger.info(f"科普库没有结果：{imdb}")
        return False
    logger.debug(merged_dict)

    quality_mapping = {
        "720p": ['4K/2160P', "1080P"],
        "480p": ['4K/2160P', "1080P", "720P"],
        "240p": [],
    }
    if quality == "240p" or any(merged_dict.get(q) for q in quality_mapping.get(quality, [])):
        logger.warning(f"{imdb} 有更高质量 {ids}：{merged_dict}")
        return True

    logger.info(f"{imdb} 无更高质量：{merged_dict}")
    return False


def log_jackett_search_results(imdb: str) -> None:
    """
    通过 IMDB 编号搜索 Jackett 并输出前几条结果。

    :param imdb: IMDB 编号
    :return: 无
    """
    logger.info(f"搜索夹克衫：{imdb}")
    response_list = get_jackett_search_response(imdb)
    if not response_list:
        return

    logger.warning(f"Jackett 搜索结果为：{len(response_list)}")
    time.sleep(0.1)
    log_top_jackett_items(response_list)


def log_top_jackett_items(data: list) -> None:
    """
    解析 Jackett 返回列表，按体积排序并记录结果。

    :param data: 结果列表
    :return: 无
    """
    from sort_movie_ops import format_bytes

    extracted_data = []

    for item in data:
        title = item.get('title', 'No Title')
        jackettindexer = item.get('jackettindexer', {})
        source_id = jackettindexer.get('@id', 'Unknown')
        size = item.get('size', '0')

        readable_size = format_bytes(size)

        extracted_data.append({
            'title': title,
            'source_id': source_id,
            'size_bytes': int(size),
            'readable_size': readable_size,
        })

    extracted_data.sort(key=lambda x: x['size_bytes'], reverse=True)

    for item in extracted_data[:5]:
        logger.info(f"{item['title']} | 来源: {item['source_id']} | 大小: {item['readable_size']}")
