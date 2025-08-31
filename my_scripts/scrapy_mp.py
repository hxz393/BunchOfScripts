"""
抓取 mp 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from retrying import retry

from my_module import read_json_to_dict, sanitize_filename, write_list_to_file

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG_PATH = 'config/scrapy_mp.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

MP_URL = CONFIG['mp_url']  # mp 地址
MP_MOVIE_URL = CONFIG['mp_movie_url']  # mp 电影列表地址
MP_COOKIE = CONFIG['mp_cookie']  # 用户甜甜
REQUEST_HEAD = CONFIG['request_head']  # 请求头
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录

REQUEST_HEAD["Cookie"] = MP_COOKIE  # 请求头加入认证


def scrapy_mp(start_page: int = 0, end="face-to-face-2") -> None:
    """
    抓取发布信息写入到文件。
    """
    logger.info("抓取 mp 站点发布信息")
    end_url = f"{MP_MOVIE_URL}movies/{end}/"
    while True:
        # 请求 mp 主页
        logger.info(f"抓取第 {start_page} 页")
        url = f"{MP_MOVIE_URL}{start_page}/"
        response = get_mp_response(url)
        result_list = parse_mp_response(response)
        # print(result_list)
        logger.info(f"共 {len(result_list)} 个结果")
        process_all(result_list, max_workers=20)

        # 检查帖子
        if end_url in (result_item['link'] for result_item in result_list):
            logger.info("没有新发布，完成")
            break

        # logger.info(f"结果：{result_list}")
        logger.warning("-" * 255)
        start_page += 1


def process_all(result_list, max_workers=5):
    """
    并发调用 visit_sk_url，result_list 中每个元素都会被提交到线程池执行。
    max_workers 控制并发线程数，视网络 I/O 或目标服务器承受能力调整。
    """
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_item = {
            executor.submit(visit_mp_url, item): item
            for item in result_list
        }
        # 按完成顺序收集结果或捕获异常
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                ret = future.result()
            except Exception as exc:
                logger.error(f"[ERROR] {item} -> {exc!r}")
            else:
                results.append(ret)
    return results


@retry(stop_max_attempt_number=15, wait_random_min=1000, wait_random_max=10000)
def get_mp_response(url: str) -> requests.Response:
    """请求流程"""
    response = requests.get(url, headers=REQUEST_HEAD)
    response.encoding = 'utf-8'
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    return response


def parse_mp_response(response: requests.Response) -> list:
    """解析流程"""
    soup = BeautifulSoup(response.text, 'html.parser')
    container = soup.find('div', id='archive-content')
    results = []
    if not container:
        return results

    for art in container.find_all('article', class_='item movies'):
        data_div = art.find('div', class_='data')
        if not data_div:
            continue

        h3_a = data_div.find('h3').find('a', href=True)
        if not h3_a:
            continue
        title = h3_a.get_text(strip=True)
        link = h3_a['href']

        span = data_div.find('span')
        year = ''
        if span:
            text = span.get_text(strip=True)
            # 圆整年份，支持如 "Jul. 20, 1990" 或 "1990"
            match = re.search(r'\b(19|20)\d{2}\b', text)
            if match:
                year = match.group(0)

        results.append({
            'title': title,
            'link': link,
            'year': year
        })

    return results


def fix_name(name: str, max_length: int = 220) -> str:
    """修剪文件名"""
    name = re.sub(r'\s*\|\s*', '，', name)
    name = re.sub(r'\s*/\s*', '｜', name)
    name = re.sub(r'\s*\\s*', '｜', name)
    name = re.sub(r'\s+', ' ', name)
    name = name.replace("\t", " ").strip()
    name = name.replace("{@}", ".").strip()
    # 长度不超限，直接返回
    if len(name) <= max_length:
        return name
    else:
        return name[:max_length]


def visit_mp_url(result_item: dict):
    """访问详情页"""
    url = result_item["link"]
    logger.info(f"访问 {url}")
    response = get_mp_response(url)
    soup = BeautifulSoup(response.text, "html.parser")
    # 提取编号
    cf = soup.find('div', class_='custom_fields2')
    if not cf:
        logger.error("没有找到 IMDB 段落")
        return

    m_id = ""
    for a in cf.find_all('a', href=True):
        href = a['href']
        # 查找 IMDb 和 TMDB ID
        imdb_match = re.search(r'(tt\d+)', href, re.IGNORECASE)
        tmdb_match = re.search(r'themoviedb\.org/movie/(\d+)', href, re.IGNORECASE)
        if imdb_match:
            m_id = imdb_match.group(1)
            break
        elif tmdb_match:
            m_id = f"tmdb{tmdb_match.group(1)}"
            break
    # 提取内容
    desc = soup.find('div', itemprop='description', class_='wp-content')
    if not desc:
        return ""

    # 将 <a> 标签替换为 "文本 (URL)"
    for a in desc.find_all('a', href=True):
        text = a.get_text(strip=True)
        href = a['href']
        replacement = f"{text} ({href})" if text else href
        a.replace_with(replacement)

    # 获取纯文本，保持标签间适当空格/换行
    text = desc.get_text(separator='\n', strip=True)
    # 文件名
    file_name = fix_name(result_item['title'])
    file_name = sanitize_filename(file_name)
    file_name = f"{file_name}({result_item['year']}) - mp [{m_id}].rare"
    path = os.path.join(OUTPUT_DIR, file_name)
    write_list_to_file(path, [url,text])
