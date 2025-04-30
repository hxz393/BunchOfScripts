"""
抓取 dhd 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import asyncio
import concurrent.futures
import hashlib
import logging
import os
import re
import urllib.parse

import aiohttp
import bencodepy
import requests
from bs4 import BeautifulSoup
from retrying import retry

from my_module import read_json_to_dict, sanitize_filename, write_list_to_file, update_json_config, read_file_to_list

requests.packages.urllib3.disable_warnings()
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
    file_name = file_name.replace("/", "｜").replace("\\", "｜")
    file_name = sanitize_filename(file_name).strip()
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
                tasks = [get_dhd_response(f"{DHD_MOVIE_URL}{page}.html", session) for page in page_numbers]
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
    download_link = ""
    if span_tag:
        a_tag = span_tag.find("a")
        if a_tag:
            # 判断 <a> 标签下是否有 <img> 且其 src 属性为指定地址
            img_tag = a_tag.find("img")
            if img_tag and img_tag.get("src", "").strip() == "static/image/filetype/torrent.gif":
                link_href = a_tag.get("href")
                if link_href:  # 确保 href 存在
                    download_link = f"{DHD_URL}/{link_href}"

    # 如果第一种方式未找到，则尝试第二种格式：<p class="attnm">
    if not download_link:
        p_tags = soup.find_all("p", class_="attnm")
        for p_tag in p_tags:
            a_tag = p_tag.find("a")
            if a_tag:
                link_href = a_tag.get("href")
                if link_href:
                    download_link = f"{DHD_URL}/{link_href}"

    return download_link


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


def dhd_to_log(directory: str=r"B:\0.整理\BT\dhd") -> None:
    """转换 dhd 文件到 log 文件，并使用多线程执行"""
    # 获取指定目录下所有以 .dhd 为后缀的文件
    file_list = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.dhd')]

    def process_file(file_path: str):
        """
        单个文件的处理流程：
        1. 读取文件中的链接
        2. 根据链接获取种子下载页面，并提取下载地址
        3. 下载种子文件，并转换为磁链
        4. 回写磁链到 .log 文件，并删除临时种子文件
        """
        # 每个线程创建自己的 requests.Session
        session = requests.Session()
        logger.info(f"处理文件：{file_path}")

        try:
            # 从 dhd 文件读取链接，假设 read_file_to_list 返回一个列表
            link = read_file_to_list(file_path)[0]
        except Exception as e:
            logger.error(f"读取文件 {file_path} 失败: {e}")
            return

        # 构造 torrent 文件保存路径
        torrent_path = os.path.join(directory, os.path.basename(file_path).replace(".dhd", ".torrent"))

        # 获取种子下载页面并提取下载地址
        response = get_dhd(session, link)
        dl_url = extract_dl_url(response.text)
        if not dl_url:
            logger.warning(f"文件 {file_path}: 没有找到下载地址")
            return

        # 下载种子文件并转换为磁链
        get_dhd_torrent(session, dl_url, torrent_path)
        magnet = torrent_to_magnet(torrent_path)
        if not magnet:
            logger.warning(f"文件 {file_path}: 转换磁链失败")
            os.remove(torrent_path)
            return

        # 回写磁链到 .log 文件，并删除原始 dhd 文件和临时 torrent 文件
        new_file_path = file_path.replace(".dhd", ".log")
        os.rename(file_path, new_file_path)
        write_list_to_file(new_file_path, [magnet])
        os.remove(torrent_path)
        logger.info(f"文件 {file_path}: 转换完成")
        logger.info("-" * 255)

    # 根据文件数量设置线程池大小，最大线程数可根据实际情况调整
    # max_workers = min(16, len(file_list)) if file_list else 1
    max_workers = min(32, len(file_list)) if file_list else 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        futures = [executor.submit(process_file, file) for file in file_list]
        # 可选：等待每个任务执行完毕，并捕获异常
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"处理文件时出现异常: {e}")


@retry(stop_max_attempt_number=15, wait_random_min=1000, wait_random_max=5000)
def get_dhd(session: requests.Session, url: str) -> requests.Response:
    """请求流程"""
    response = session.get(url, timeout=10, verify=False, headers=REQUEST_HEAD)
    response.encoding = 'gbk'
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    return response


@retry(stop_max_attempt_number=15, wait_random_min=1000, wait_random_max=5000)
def get_dhd_torrent(session: requests.Session, url: str, torrent_path: str) -> None:
    """下载种子"""
    response = session.get(url, timeout=10, verify=False, headers=REQUEST_HEAD)
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    # 将文件内容保存为 torrent 文件
    with open(torrent_path, "wb") as file:
        file.write(response.content)


def torrent_to_magnet(torrent_file_path: str) -> str:
    """
    将 torrent 文件转换为磁链
    """
    # 读取 torrent 文件二进制内容
    with open(torrent_file_path, 'rb') as f:
        torrent_data = f.read()

    # 解析 bencoded 数据
    torrent_dict = bencodepy.decode(torrent_data)

    # 提取 info 字典，它包含了实际内容信息
    info = torrent_dict[b'info']

    # 重新对 info 字典进行 bencode 编码，并计算 SHA1 哈希值作为 info hash
    info_bencoded = bencodepy.encode(info)
    info_hash = hashlib.sha1(info_bencoded).hexdigest()

    # 可选：提取 torrent 的显示名称（如果存在）
    display_name = ""
    if b'name' in info:
        try:
            display_name = info[b'name'].decode('utf-8')
        except UnicodeDecodeError:
            display_name = info[b'name'].decode('latin1')
    display_name_encoded = urllib.parse.quote(display_name) if display_name else ""

    # 可选：提取 tracker 信息，优先使用 announce-list 中的第一个 tracker，如果没有则使用 announce 字段
    tracker_url = ""
    if b'announce-list' in torrent_dict:
        # announce-list 通常是个嵌套列表，取第一个 tracker
        try:
            tracker_url = torrent_dict[b'announce-list'][0][0].decode('utf-8')
        except Exception:
            tracker_url = ""
    elif b'announce' in torrent_dict:
        tracker_url = torrent_dict[b'announce'].decode('utf-8')

    tracker_url_encoded = urllib.parse.quote(tracker_url) if tracker_url else ""

    # 构造磁链，至少包含 xt 参数（info hash）
    magnet_link = f"magnet:?xt=urn:btih:{info_hash}"

    # 如果有名称，则添加 dn 参数
    if display_name_encoded:
        magnet_link += f"&dn={display_name_encoded}"

    # 如果有 tracker 则添加 tr 参数
    if tracker_url_encoded:
        magnet_link += f"&tr={tracker_url_encoded}"

    return magnet_link
