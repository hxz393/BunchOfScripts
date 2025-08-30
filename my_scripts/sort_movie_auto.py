"""
自动化整理电影和导演文件夹

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import re
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from get_director_movies import get_tmdb_director_movies_all
from my_module import read_file_to_list, write_list_to_file, read_json_to_dict, remove_target
from scrapy_kpk import scrapy_kpk, scrapy_jeckett
from sort_movie import sort_movie
from sort_movie_director import sort_movie_director
from sort_movie_mysql import insert_movie_wanted
from sort_movie_ops import get_ids, safe_get, scan_ids, get_files_with_extensions, get_subdirs, parse_jason_file_name, delete_trash_files, check_local_torrent, move_all_files_to_root
from sort_movie_request import get_imdb_movie_details, get_tmdb_search_response, get_tmdb_director_details, get_douban_response, get_douban_search_details, get_tmdb_movie_details
from sort_ru import ru_search

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict('config/sort_movie.json')  # 配置文件

TMDB_PERSON_URL = CONFIG['tmdb_person_url']  # tmdb 导演地址
TMDB_MOVIE_URL = CONFIG['tmdb_movie_url']  # tmdb 电影地址

IMDB_PERSON_URL = CONFIG['imdb_person_url']  # imdb 导演地址
IMDB_MOVIE_URL = CONFIG['imdb_movie_url']  # imdb 电影地址


def sort_director_auto(path: str) -> None:
    """
    自动整理导演目录，生成导演别名空文件

    :param path: 导演目录
    :return: 无
    """
    # 查找 imdb 编号
    result_list = [path]
    director_main = os.path.basename(path)
    logger.info(f"开始处理：{director_main}")
    p = Path(path)
    file_list = [str(item) for item in p.rglob('*')]
    imdb_list = []
    for file_path in file_list:
        imdb_id = m.group(1) if (m := re.search(r'(tt\d+)', file_path)) else None
        if imdb_id:
            imdb_list.append(imdb_id)
    if not imdb_list:
        logger.error(f"目录内没有收集到 IMDB 编号: {director_main}")
        time.sleep(0.5)
        return
    imdb_list = list(set(imdb_list))
    director_ids = scan_ids(path)

    # 搜索 imdb，获取导演链接
    nm_id = director_ids['imdb']
    if not nm_id:
        for imdb_id in imdb_list:
            r = get_imdb_director(imdb_id, director_main)
            if r:
                result_list.append(r)
                nm_id = m.group(1) if (m := re.search(r'(nm\d+)', r)) else None
                break
    else:
        logger.info(f"IMDB 编号：{nm_id}")
    if not nm_id:
        logger.error(f"IMDB 电影导演不匹配或没有导演 {director_main}")
        return

    # 搜索 tmdb，获取导演链接
    tmdb_id = director_ids['tmdb']
    if not tmdb_id:
        tmdb_id = get_tmdb_director(nm_id, director_main, imdb_list)
        if tmdb_id:
            result_list.append(tmdb_id)
    else:
        logger.info(f"TMDB 编号：{tmdb_id}")
    if not tmdb_id:
        logger.error(f"没有在 tmdb 找到导演链接 {director_main}")

    # 搜索 douban，获取导演链接
    douban_id = director_ids['douban']
    if not douban_id:
        douban_id = get_douban_director(nm_id)
        if douban_id:
            result_list.append(douban_id)
    else:
        logger.info(f"DOUBAN 编号：{douban_id}")
    if not douban_id:
        logger.error(f"没有在 douban 找到导演链接 {director_main}")

    # 将结果写入到文件，执行自动抓取
    logger.info(f"查询结果：{result_list}\n")
    target_file = r'B:\2.脚本\!00.txt'
    write_list_to_file(target_file, result_list)
    get_ids(target_file)
    done = sort_movie_director(read_file_to_list(target_file)[0])
    if done == 2:
        shutil.move(path, os.path.join(r'A:\0b.导演别名', director_main))


def get_imdb_director(movie_id: str, director_main: str) -> Optional[str]:
    """
    搜索 imdb，获取导演信息

    :param movie_id: imdb 编号
    :param director_main: 导演主要名字
    :return: 搜索结果，成功则返回导演链接，失败返回 None
    """
    m = get_imdb_movie_details(movie_id)
    if not m:
        return

    # 获取导演列表，这个结构有点特别，需要分步处理
    directors_list = safe_get(
        m,
        ["props", "pageProps", "aboveTheFoldData", "directorsPageTitle"],
        default=[]
    )
    if len(directors_list) > 1:
        logger.error(f"导演列表有多个元素：{directors_list}")
        sys.exit(1)
    first_item = directors_list[0] if directors_list else {}
    credits_list = safe_get(first_item, ["credits"], default=[])

    # 提取列表中的导演和编号
    directors = []
    for credit in credits_list:
        name = safe_get(credit, ["name", "nameText", "text"], default="")
        nm_id = safe_get(credit, ["name", "id"], default="")
        directors.append({
            "name": name,
            "link": f"{IMDB_PERSON_URL}/{nm_id}"
        })

    # 尝试匹配传入的导演主名字，匹配到了就返回
    for d in directors:
        if d["name"] and director_main.lower() in d["name"].lower():
            return d["link"]
        else:
            logger.warning(f"没有匹配到导演，查询到导演名字：{d['name']}")

    # 如果没有匹配到导演的名字返回 None
    return None


def get_tmdb_director_aka(tmdb_id: str, director_main: str) -> Optional[str]:
    """获取导演详细信息，得到别名列表，然后匹配

    :param tmdb_id: tmdb 编号
    :param director_main: 导演主要名字
    :return: 匹配，成功则返回导演链接，失败返回 None
    """
    p = get_tmdb_director_details(tmdb_id)
    aka_org = list(p["also_known_as"])
    aka_org.append(p['name'])
    aka = [i.lower().replace(" ", "") for i in aka_org]
    link = f"{TMDB_PERSON_URL}/{tmdb_id}"
    if director_main.lower().replace(" ", "") in aka:
        return link
    else:
        logger.warning(f"没有匹配到导演，查询到导演名字：{aka_org} {link}")
        return None


def get_tmdb_director(nm_id: str, director_main: str, imdb_list: list) -> Optional[str]:
    """
    两种方式搜索 tmdb，获取导演信息

    :param nm_id: nm 导演编号
    :param director_main: 导演主要名字
    :param imdb_list: 通过 imdb 列表搜索电影获取导演信息，备用方式
    :return: 搜索结果，成功则返回导演链接，失败返回 None
    """
    # 搜索导演，获取导演信息
    data = get_tmdb_search_response(nm_id)
    if not data:
        return

    persons = data.get("person_results", [])
    if persons:
        # 一般只有一个结果
        person = persons[0]
        tmdb_id = person.get('id')
        return f"{TMDB_PERSON_URL}/{tmdb_id}"
    else:
        logger.warning(f"没有在 TMDB 上搜索到导演：{nm_id}，尝试通过电影获取导演")

    # 麻烦的方法
    for imdb in imdb_list:
        r = get_tmdb_search_response(imdb)
        if r:
            movie_results = r.get('movie_results', [])
            if not movie_results:
                continue
            movie_id = movie_results[0].get('id')
            movie_details = get_tmdb_movie_details(movie_id)
            crew_list = movie_details['casts'].get('crew', [])
            # 在 crew 中筛选出 job == 'Director' 的人员
            directors = []
            for member in crew_list:
                if member.get('job') == 'Director':
                    member_id = member.get('id')
                    directors.append({
                        "name_id": member_id,
                        "name": member.get('name'),
                        "link": f"{TMDB_PERSON_URL}/{member_id}"
                    })
            # 尝试匹配 director_main
            for d in directors:
                if d["name"] and d["name"].lower() == director_main.lower():
                    return d["link"]
                else:
                    result = get_tmdb_director_aka(d["name_id"], director_main)
                    if result:
                        return result
    return


def get_douban_director(nm_id: str) -> Optional[str]:
    """
    搜索 douban，获取导演信息

    :param nm_id: imdb 导演编号
    :return: 搜索结果，成功则返回导演链接，失败返回 None
    """
    # 搜索编号，获取结果
    r = get_douban_response(nm_id, "director_search")
    if not r:
        logger.error("没有获取到豆瓣搜索响应")
        return

    return get_douban_search_details(r)


def sort_movie_auto(path: str, target_file: str) -> None:
    """
    自动整理电影，输出链接到文本文件

    :param path: 导演目录
    :param target_file: 临时文本文件
    :return: 无
    """
    folders = [os.path.join(path, item) for item in os.listdir(path) if os.path.isdir(os.path.join(path, item))]
    if not folders:
        logger.error(f"目录下没有子文件夹 {path}")
        return

    for folder in folders:
        logger.info(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 开始处理：{folder}")
        logger.info("-" * 25 + "步骤：搜索电影" + "-" * 25)
        r = sort_movie_auto_folder(folder, target_file)
        if r:
            logger.error(r)
            logger.warning("=" * 255)
            return
        # 先移除文件层级
        move_all_files_to_root(folder)
        time.sleep(0.1)
        logger.info("-" * 25 + "步骤：抓取电影信息" + "-" * 25)
        get_ids(target_file)
        url_list = read_file_to_list(target_file)
        sort_movie(url_list[0])
        time.sleep(0.1)
        logger.warning("=" * 255)
        time.sleep(0.1)


def sort_movie_auto_folder(path: str, target_file: str) -> Optional[str]:
    """
    自动整理电影

    :param path: 电影目录
    :param target_file: 来源文本文件
    :return: 失败时返回原因
    """
    result_list = [path]
    imdb_id = m.group(1) if (m := re.search(r'(tt\d+)', path)) else None
    if not imdb_id:
        return f"目录缺少 IMDB 编号 {path}"
    imdb_url = f"{IMDB_MOVIE_URL}/{imdb_id}/"
    result_list.append(imdb_url)

    # 搜索 tmdb，获取链接
    r = get_tmdb_id(imdb_id)
    if r["result"]:
        logger.error(r["result"])
    tmdb_url = r["tmdb_url"]
    result_list.append(tmdb_url)

    # 搜索豆瓣，获取链接
    r = get_douban_id(imdb_id)
    if r["result"]:
        logger.error(r["result"])
    douban_url = r["douban_url"]
    result_list.append(douban_url)

    # 将结果写入到文件
    write_list_to_file(target_file, result_list)


def get_tmdb_id(imdb_id: str) -> dict:
    """
    搜索tmdb，获取 tmdb 电影链接

    :param imdb_id: imdb 编号
    :return: 搜索结果
    """
    return_dict = {"result": "", "tmdb_url": ""}
    # 搜索 tmdb，获取响应
    search_result = get_tmdb_search_response(imdb_id)
    if not search_result.get('movie_results'):
        return_dict["result"] = f"tmdb 搜索失败，没找到电影"
        return return_dict

    # 取第一条结果
    movie_id = search_result.get('movie_results')[0].get('id')
    if not movie_id:
        return_dict["result"] = f"获取 tmdb id 失败"
        return return_dict

    return_dict["tmdb_url"] = f"{TMDB_MOVIE_URL}/{movie_id}"
    return return_dict


def get_douban_id(imdb_id: str) -> dict:
    """
    搜索豆瓣，获取豆瓣电影链接

    :param imdb_id: imdb 编号
    :return: 搜索结果
    """
    return_dict = {"result": "", "douban_url": ""}
    # 搜索豆瓣，获取响应
    response = get_douban_response(imdb_id, "movie_search")
    if not response:
        return_dict["result"] = "豆瓣电影搜索失败"
        return return_dict

    # 解析响应内容
    inner_url = get_douban_search_details(response)
    if not inner_url:
        return return_dict

    return_dict["douban_url"] = inner_url
    return return_dict


def sort_ru_auto(source_path: str, target_path: str, max_workers: int = 8) -> None:
    """
    使用多线程并发自动搜索下载，如果有搜索结果则放弃继续搜索，否则移动到 target_path

    :param source_path: 来源目录
    :param target_path: 目标目录
    :param max_workers: 最大线程数
    :return: 无
    """

    def search_and_move(name, path):
        """搜索函数"""
        _, result = ru_search(name)
        logger.info(f"搜索结果 {result}：{path}")
        if not result:
            shutil.move(path, target_path)

    keyword_dict = {entry.name: entry.path for entry in os.scandir(source_path) if entry.is_dir()}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(search_and_move, k, v) for k, v in keyword_dict.items()]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"线程执行异常：{e}")


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


def achieve_director(path: str) -> None:
    """
    整理没收集的电影信息，额外去搜索下载

    :param path: 导演目录
    :return: 无
    """
    director_name = os.path.basename(path)
    logger.info(f"处理导演归档：{director_name}")
    time.sleep(0.1)
    # 获取没有资源的电影，存到 wanted 表中
    result = get_tmdb_director_movies_all(path, pass_exists=True)
    if not result:
        return
    logger.info(f"缺少电影列表：{result}")
    insert_movie_wanted(result)
    imdb_to_title = {entry['imdb']: entry['year'] + " - " + entry['titles'][0] for entry in result}

    # 去科普库搜索有 imdb 编号的电影
    query_imdb_list = []
    for j in result:
        imdb = j.get("imdb")
        if imdb:
            query_imdb_list.append(imdb)
    if not query_imdb_list:
        return
    logger.info(f"查询列表：{query_imdb_list}")
    logger.info("-" * 55)

    for imdb in query_imdb_list:
        quality = "240p"
        source = "None"
        logger.info(f"标题：{imdb_to_title.get(imdb)}")
        scrapy_kpk(imdb, quality)
        scrapy_jeckett(imdb)
        result = check_local_torrent(imdb, quality, source)
        move_counts = result['move_counts']
        if move_counts:
            logger.warning(f"{imdb} 请检查本地库存: {move_counts}")
            time.sleep(0.1)
        logger.info("-" * 35 + director_name + "-" * 35 )
        time.sleep(0.1)


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
