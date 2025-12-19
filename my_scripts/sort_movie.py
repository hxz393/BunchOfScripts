"""
从三大网站抓取电影信息，重命名电影文件夹，保存信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import sys
import time

from bs4 import BeautifulSoup
from retrying import retry

from my_module import sanitize_filename, write_dict_to_json, read_json_to_dict
from sort_movie_mysql import sort_movie_mysql
from sort_movie_ops import scan_ids, safe_get, build_movie_folder_name, merged_dict, create_aka_movie, get_video_info, check_movie, get_movie_id, fix_douban_name
from sort_movie_request import get_tmdb_movie_details, get_imdb_movie_details, get_douban_response, get_tmdb_movie_cover

logger = logging.getLogger(__name__)


def sort_movie(path: str, tv: bool = False) -> None:
    """
    从三大网站抓取信息

    :param path: 电影目录路径
    :param tv: 是否是电视剧
    :return: 无
    """
    path = path.strip()
    movie_info_file = f"{path}\\movie_info.json5"
    no_movie_info = not os.path.exists(movie_info_file)  # 不存在 movie_info.json5 文件
    if not os.path.exists(path):
        logger.error("目录不存在")
        return

    # 检查是否没有任何 ID
    movie_ids = scan_ids(path)
    no_movie_ids = all(value is None for value in movie_ids.values())  # 不存在 id 文件
    if no_movie_info and no_movie_ids:
        logger.error("没有找到任何 ID")
        return

    # 初始化变量
    local_only = not no_movie_info and no_movie_ids  # 不去网络搜索电影信息
    tv = True if movie_ids["tmdb"] and movie_ids["tmdb"].find('tv') != -1 else False
    if local_only:
        logger.warning("本地三无信息处理模式")
        movie_info = read_json_to_dict(movie_info_file)
    else:
        movie_info = {
            "director": "",
            "year": 0,
            "original_title": "",
            "chinese_title": "",
            "genres": [],
            "country": [],
            "language": [],
            "runtime": 0,
            "poster_path": "",
            "titles": [],
            "directors": [],
            "version": "",
            "publisher": "",
            "pubdate": "",
            "dvhdr": "",
            "audio": "未知",
            "subtitle": "未知",
            "comment": None
        }

    # 三大网站处理流程
    if not local_only:
        actions = {
            'tmdb': lambda tmdb_id: get_tmdb_movie_info(tmdb_id.replace("tv", ""), movie_info, tv),
            'imdb': lambda imdb_id: get_imdb_movie_info(imdb_id, movie_info),
            'douban': lambda douban_id: get_douban_movie_info(douban_id, movie_info)
        }
        for key, action in actions.items():
            id_value = movie_ids.get(key)
            if id_value:
                action(id_value)
            else:
                logger.warning(f"没有 {key.upper()} 编号。")

    # 本地处理视频文件，获取视频基础信息
    file_info = get_video_info(path)
    if not file_info:
        return

    # 合并处理字典，加入新字段
    movie_dict = merged_dict(path, movie_info, movie_ids, file_info)
    # 拼凑文件名
    new_path = os.path.join(os.path.dirname(path), sanitize_filename(build_movie_folder_name(path, movie_dict)))
    # 重命名目录
    os.rename(path, new_path)
    # 打印信息
    logger.info(f"抓取结果：{movie_dict}")
    # 下载图片
    image_path = os.path.join(new_path, f"{get_movie_id(movie_dict)}.jpg")
    if not os.path.exists(image_path):
        get_tmdb_movie_cover(movie_dict["poster_path"], image_path)
    # 建立电影别名空文件
    create_aka_movie(new_path, movie_dict)
    # 写入信息到本地
    write_dict_to_json(os.path.join(new_path, "movie_info.json5"), movie_dict)

    # 最后检查目录规范
    time.sleep(0.1)
    logger.info("-" * 25 + "步骤：检查校验信息" + "-" * 25)
    time.sleep(0.1)
    check_result = check_movie(new_path)
    if check_result:
        logger.error(check_result)
        return

    # 没有问题才将信息插入数据库
    sort_movie_mysql(new_path)
    time.sleep(0.1)
    logger.info(f"旧名：{path}")
    logger.info(f"新名：{new_path}")


@retry(stop_max_attempt_number=50, wait_random_min=1000, wait_random_max=5000)
def get_tmdb_movie_info(movie_id: str, movie_info: dict, tv: bool) -> None:
    """
    从 TMDB 获取电影信息，储存到传入的字典中

    :param movie_id: 电影 tmdb 编号
    :param movie_info: 电影信息字典，原地修改
    :param tv: 是否是电视剧
    :return: 无
    """
    m = get_tmdb_movie_details(movie_id, tv)
    if not m:
        raise Exception("没有获取到电影信息，重试一次")

    movie_info["genres"] = [i['name'] for i in m['genres']] if m.get('genres') else []
    movie_info["country"] = [i for i in m['origin_country']]
    movie_info["language"] = [m['original_language']]
    movie_info["original_title"] = m['original_name' if tv else 'original_title']
    movie_info["year"] = m['first_air_date' if tv else 'release_date']
    movie_info["year"] = movie_info["year"][:4] if movie_info["year"] else None
    runtime_tmdb = 0
    if tv:
        runtime = m['last_episode_to_air']['runtime']
        if runtime:
            runtime_tmdb = runtime * m['last_episode_to_air']['episode_number']
    else:
        runtime_tmdb = m['runtime']
    movie_info["runtime"] = runtime_tmdb
    movie_info["runtime_tmdb"] = runtime_tmdb

    if tv:
        credits_list = m.get('credits', {})
        crew_list = credits_list.get('crew', [])
        movie_info["directors"] = [member.get('original_name') for member in crew_list if member.get('known_for_department') == 'Directing']
        movie_info["directors"].extend([member.get('name') for member in crew_list if member.get('known_for_department') == 'Directing'])
        original_names = [creator.get('original_name') for creator in m.get('created_by', [])]
        english_names = [creator.get('name') for creator in m.get('created_by', [])]
        movie_info["directors"].extend([name for name in original_names if name is not None])
        movie_info["directors"].extend([name for name in english_names if name is not None])
    else:
        cast_list = m.get('casts', {})
        crew_list = cast_list.get('crew', [])
        movie_info["directors"] = [member.get('original_name') for member in crew_list if member.get('job') == 'Director']
        movie_info["directors"].extend([member.get('name') for member in crew_list if member.get('job') == 'Director'])

    translations_list = m.get('translations', {}).get('translations', [])
    m_key = 'name' if tv else 'title'
    movie_info["chinese_title"] = next((item['data'][m_key] for item in translations_list if item.get('iso_3166_1') == 'CN'), "")
    movie_info["titles"].extend([item['data'][m_key] for item in translations_list if item['data'][m_key]])
    movie_info["titles"].extend([item.get('title', []) for item in m.get('titles', {})])
    movie_info["titles"].append(m[m_key])
    movie_info["titles"].append(movie_info["original_title"])

    movie_info["poster_path"] = m['poster_path']


def get_imdb_movie_info(movie_id: str, movie_info: dict) -> None:
    """
    从 IMDB 获取电影信息，储存到传入的字典中

    :param movie_id: 电影 imdb 编号
    :param movie_info: 电影信息字典，原地修改
    :return: 无
    """
    m = get_imdb_movie_details(movie_id)
    if not m:
        return

    # 解析 JSON 数据，用到一个辅助函数，防止解析异常
    # 获取年份
    if not movie_info["year"]:
        year = safe_get(m, ["props", "pageProps", "aboveTheFoldData", "releaseYear", "year"], default=0)
        movie_info["year"] = year

    # 获取时长
    runtime = safe_get(m, ["props", "pageProps", "aboveTheFoldData", "runtime", "seconds"], default=0)
    runtime_imdb = int(runtime / 60)
    if not movie_info["runtime"]:
        movie_info["runtime"] = runtime_imdb
    movie_info["runtime_imdb"] = runtime_imdb

    # 获取原名
    original_title = safe_get(m, ["props", "pageProps", "aboveTheFoldData", "originalTitleText", "text"], default="")
    movie_info["titles"].append(original_title)
    if not movie_info["original_title"]:
        movie_info["original_title"] = original_title

    # 获取别名
    aka_edges = safe_get(m, ["props", "pageProps", "mainColumnData", "akas", "edges"], default=[])
    first_edges_item = aka_edges[0] if aka_edges else {}
    aka_title = safe_get(first_edges_item, ["node", "text"], default="")
    if aka_title:
        movie_info["titles"].append(aka_title)

    # 获取风格标签，直接合并到原表
    genre_texts = [
        safe_get(item, ["genre", "text"], default="")
        for item in
        safe_get(m, ["props", "pageProps", "aboveTheFoldData", "titleGenres", "genres"], default=[])
    ]
    movie_info["genres"].extend(genre_texts)

    # 获取国家，直接合并
    country_ids = [
        safe_get(item, ["id"], default="")
        for item in
        safe_get(m, ["props", "pageProps", "aboveTheFoldData", "countriesOfOrigin", "countries"], default=[])
    ]
    movie_info["country"].extend(country_ids)

    # 获取语言列表，直接合并
    languages = [
        safe_get(item, ["id"], default="")
        for item in
        safe_get(m, ["props", "pageProps", "mainColumnData", "spokenLanguages", "spokenLanguages"], default=[])
    ]
    movie_info["language"].extend(languages)

    # 获取导演列表，这个结构有点特别，需要分步处理
    # 1.获取 directorsPageTitle ，结果为一个列表
    directors_list = safe_get(
        m,
        ["props", "pageProps", "aboveTheFoldData", "directorsPageTitle"],
        default=[]
    )
    # 2.directors_list 可能是空列表，也可能有多个元素。遇见多个元素开眼看看
    if len(directors_list) > 1:
        logger.error(f"导演列表有多个元素：{directors_list}")
        sys.exit(1)
    # 3.如果列表不为空，则取第一个元素，否则用空字典
    first_item = directors_list[0] if directors_list else {}
    # 4.在 first_item 中安全获取 credits
    credits_list = safe_get(first_item, ["credits"], default=[])
    # 5.提取导演名称，用列表装多个
    directors = [
        safe_get(credit, ["name", "nameText", "text"], default="")
        for credit in credits_list
    ]
    # 6.最后把结果合并到 movie_info 里
    movie_info["directors"].extend(directors)


def get_douban_movie_info(movie_id: str, movie_info: dict) -> None:
    """
    从 DOUBAN 获取电影信息，储存到传入的字典中

    :param movie_id: 电影 douban 编号
    :param movie_info: 电影信息字典，原地修改
    :return: 无
    """
    response = get_douban_response(movie_id, "movie_response")
    if not response:
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    info_div = soup.find("div", id="info")
    if not info_div:
        sys.exit(f"豆瓣页面解析失败")

    # 获取电影原名
    original_title = ""
    a_tag = soup.find('a', class_='nbgnbg')
    if a_tag:
        img_tag = a_tag.find('img')
        if img_tag and 'alt' in img_tag.attrs:
            original_title = img_tag['alt']
            movie_info["titles"].append(original_title)
            if not movie_info["original_title"]:
                movie_info["original_title"] = original_title

    # 获取电影中文名
    chinese_title = soup.find("title").get_text(strip=True).replace("(豆瓣)", "").strip()
    if chinese_title != original_title:
        movie_info["titles"].append(chinese_title)
        if not movie_info["chinese_title"] and original_title and chinese_title != original_title:
            movie_info["chinese_title"] = chinese_title

    # 获取导演列表
    director_tag = info_div.find("span", class_="pl", string="导演")
    if director_tag:
        director_attrs = director_tag.find_next("span", class_="attrs")
        movie_info["directors"].extend([a.get_text(strip=True) for a in director_attrs.find_all("a")])

    # 获取类型
    genre_tags = info_div.find_all("span", property="v:genre")
    movie_info["genres"].extend([tag.get_text(strip=True) for tag in genre_tags])

    # 制片国家
    country_tag = info_div.find("span", class_="pl", string="制片国家/地区:")
    if country_tag and country_tag.next_sibling:
        movie_info["country"].extend(country_tag.next_sibling.strip().split("/"))

    # 语言
    if not movie_info["language"]:
        language_tag = info_div.find("span", class_="pl", string="语言:")
        if language_tag and language_tag.next_sibling:
            movie_info["language"].extend(language_tag.next_sibling.strip().split("/"))

    # 上映日期
    if not movie_info["year"]:
        release_date_tag = info_div.find("span", property="v:initialReleaseDate")
        if release_date_tag:
            movie_info["year"] = release_date_tag.get_text(strip=True)[:4]

    # 提取片长
    if not movie_info["runtime"]:
        runtime_tag = info_div.find("span", property="v:runtime")
        if runtime_tag:
            movie_info["runtime"] = int(''.join(filter(str.isdigit, runtime_tag.get_text(strip=True))))

    # 提取别名
    aka_tag = info_div.find("span", class_="pl", string="又名:")
    if aka_tag and aka_tag.next_sibling:
        aka_name = aka_tag.next_sibling.strip()
        aka_names = aka_name.split("/")
        alias = [fix_douban_name(name) for name in aka_names]
        movie_info["titles"].extend(alias)
