"""
抓取 dhd 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import asyncio
import logging
import os
import re

import aiohttp
from bs4 import BeautifulSoup

from my_module import read_json_to_dict, sanitize_filename, write_list_to_file, update_json_config

logger = logging.getLogger(__name__)

CONFIG_PATH = 'config/scrapy_dhd.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

DHD_URL = CONFIG['dhd_url']  # ttg 地址
DHD_MOVIE_URL = CONFIG['dhd_movie_url']  # ttg 电影列表地址
DHD_DL_URL = CONFIG['dhd_dl_url']  # ttg 下载地址
NEWEST_ID = CONFIG['newest_id']  # 最新 id
DHD_COOKIE = CONFIG['dhd_cookie']  # 用户甜甜
REQUEST_HEAD = CONFIG['request_head']  # 请求头
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录
THREAD_NUMBER = CONFIG['thread_number']  # 并发数

REQUEST_HEAD["Cookie"] = DHD_COOKIE  # 请求头加入认证


async def get_dhd_response(url: str, session: aiohttp.ClientSession, retries: int = 15) -> str:
    """
    异步请求 URL，重试多次后返回响应文本
    """
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=15) as response:
                if response.status != 200:
                    raise Exception(f"请求失败，状态码：{response.status}")
                # 指定编码为 gbk，忽略错误
                text = await response.text(encoding='gbk', errors='replace')
                return text
        except Exception as e:
            logger.error(f"请求 {url} 出错，重试 {attempt + 1}/{retries}。错误：{e}")
            await asyncio.sleep(1)  # 简单延时，可根据需要实现指数退避
    raise Exception(f"请求 {url} 连续 {retries} 次失败")


def parse_dhd_response(response_text: str) -> list:
    """
    解析页面 HTML，返回结果列表。
    """
    soup = BeautifulSoup(response_text, 'html.parser')
    results = []

    # 查找所有代表电影信息的 div 元素
    for topic in soup.find_all("div", class_="topic media topic-visited"):
        # 电影名称及地址：在 title media-heading 中的 a 标签
        title_tag_h = topic.find("div", class_="title media-heading")
        if not title_tag_h:
            logger.warning("解析失败：没有找到 title media-heading")
            continue

        title_tag = title_tag_h.find("a")
        if title_tag:
            name = title_tag.get_text(strip=True)
            url_href = title_tag.get("href")
            dhd_id = url_href.replace("_11.html", "")
            url = f"{DHD_URL}/{url_href}"
        else:
            logger.error("解析失败：没有找到 title media-heading 中的 a 标签")
            continue

        results.append({"name": name, "url": url, "id": dhd_id})

    return results


async def working_dhd_async(info: dict, session: aiohttp.ClientSession) -> None:
    """
    异步抓取单个信息项
    使用异步方式获取页面后，再调用同步的处理函数完成后续操作。
    """
    name = info["name"]
    url = info["url"]
    response_text = await get_dhd_response(url, session)
    imdb = extract_imdb_id(response_text)
    dl_url = extract_dl_url(response_text)
    content = [url, dl_url]
    file_name = f"{name}[{imdb}].dhd"
    file_name = rename_file(file_name)
    file_name = sanitize_filename(file_name).strip()
    file_name = file_name.replace("/", "｜").replace("\\", "｜")
    file_path = os.path.join(OUTPUT_DIR, file_name)
    write_list_to_file(file_path, content)


async def scrapy_dhd_async(start_page: int = 1) -> None:
    """
    异步抓取 dhd 站点发布信息，采用 aiohttp 并发请求，同时批量抓取多个页面。
    """
    logger.info("抓取 dhd 站点发布信息")
    max_ids = []
    pages_per_batch = 1  # 每次批量请求1个页面，合计约100个链接

    connector = aiohttp.TCPConnector(ssl=False, limit=THREAD_NUMBER)
    async with aiohttp.ClientSession(connector=connector, headers=REQUEST_HEAD, trust_env=True) as session:
        while True:
            # 为当前批次增加重试逻辑
            batch_attempt = 0
            max_batch_attempts = 5
            combined_result_list = []
            while batch_attempt < max_batch_attempts:
                logger.info(f"批量抓取第 {start_page} 到 {start_page + pages_per_batch - 1} 页，尝试次数 {batch_attempt + 1}")
                page_numbers = [start_page + i for i in range(pages_per_batch)]
                tasks = [get_dhd_response(f"{DHD_MOVIE_URL}{page}", session) for page in page_numbers]
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                combined_result_list = []
                for resp in responses:
                    if isinstance(resp, Exception):
                        logger.error(f"页面请求失败：{resp}")
                        continue
                    if not resp:
                        continue
                    result_list = parse_dhd_response(resp)
                    combined_result_list.extend(result_list)
                if combined_result_list:
                    break  # 请求到内容，跳出重试循环
                else:
                    batch_attempt += 1
                    logger.error(f"批量请求返回空结果，第 {batch_attempt} 次重试")
                    await asyncio.sleep(3)  # 重试前延时
            if not combined_result_list:
                # 连续多次重试后依然为空，认为请求出错，不再继续
                logger.error("连续多次重试后，批量请求依然返回空结果")
                raise Exception("批量请求失败")

            # 比较最新 id，过滤结果
            new_list = [i for i in combined_result_list if int(i['id']) > NEWEST_ID]
            if len(new_list) == 0:
                logger.info("没有新发布")
                break

            # 并发抓取每个新链接的详情
            tasks = [working_dhd_async(item, session) for item in new_list]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for item, result in zip(new_list, results):
                if isinstance(result, Exception):
                    logger.error(f"抓取出错：{item['url']}，错误：{result}")

            start_page += pages_per_batch
            max_ids.append(max(int(i['id']) for i in new_list))

    max_id = max(max_ids) if max_ids else NEWEST_ID
    update_json_config(CONFIG_PATH, "newest_id", max_id)


def rename_file(filename: str) -> str:
    """使用正则规范化文件名"""
    pattern = r'^(?P<chinese>[^.]+)\.(?P<original>.*?)\.(?P<year>\d{4})\.(?P<remaining>.*)(?P<imdb>\[[^\]]*\])\.dhd$'
    m = re.match(pattern, filename)
    if not m:
        return filename

    chinese = m.group('chinese')
    original = m.group('original')
    year = m.group('year')
    remaining = m.group('remaining')
    imdb = m.group('imdb')

    # 校验中文名部分不能包含英文字母
    if re.search(r'[A-Za-z]', chinese):
        return filename

    # 校验原名部分不能包含中文字符
    if re.search(r'[\u4e00-\u9fff]', original):
        return filename

    # 替换原名中的点为空格
    processed_original = original.replace('.', ' ')

    # 判断 remaining 部分是否以空格结尾，若没有，则插入一个空格以保证中文名插入前有间隔
    spacer = "" if remaining.endswith(" ") else " "

    # 重新组装文件名：
    new_filename = f"{processed_original} {year}.{remaining}{spacer}[{chinese}]{imdb}.dhd"
    return new_filename


def extract_dl_url(txt: str) -> str:
    """获取下载地址"""
    soup = BeautifulSoup(txt, 'html.parser')
    span_tag = soup.find("span", attrs={"style": "white-space: nowrap"})
    if span_tag:
        a_tag = span_tag.find("a")
        if a_tag:
            link_href = a_tag.get('href')
            download_link = f"{DHD_URL}/{link_href}"
            return download_link
    return ""


def extract_imdb_id(txt: str) -> str:
    """获取 IMDB 编号"""
    pattern = r'imdb.com/title/(tt\d+)'
    match = re.search(pattern, txt)
    if match:
        return match.group(1)
    else:
        return ""


def fix_name(name: str, max_length: int = 230) -> str:
    """修剪文件名"""
    name = re.sub(r'\s*\|\s*', '，', name)
    name = re.sub(r'\s*/\s*', '｜', name)
    name = re.sub(r'\s*\\s*', '｜', name)
    name = re.sub(r'\s+', ' ', name)
    name = name.replace("\t", " ").strip()
    if len(name) <= max_length:
        return name
    else:
        return name[:max_length]


def write_to_disk(result_list: list) -> None:
    """写入到磁盘"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    for i in result_list:
        name = i['name']
        name = fix_name(name)
        name = sanitize_filename(name)
        file_name = f"{name}({i['size']})[{i['imdb']}].ttg"
        path = os.path.join(OUTPUT_DIR, file_name)
        links = [i["url"], i["dl"]]
        write_list_to_file(path, links)
