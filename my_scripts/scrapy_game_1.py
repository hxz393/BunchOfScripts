# coding=utf-8
"""
这是一个Python文件，该文件包含了几个函数，用于处理网页链接的抓取、解析以及结果的存储。

函数 handle_result 处理结果并进行存储，接收结果对象和链接作为输入参数，没有返回值。

函数 error_callback 处理进程中的错误，接收错误对象和链接作为输入参数，没有返回值。

函数 scrapy_game_1 对指定的链接进行处理，接收包含链接的范围和线程数作为输入参数，没有返回值。

函数 main 作为主程序运行，接收一个网页链接，返回链接，标题和百度链接带提取码。

函数 fetch_web_page 获取网页HTML内容，接收要访问的网页链接，返回网页的HTML内容或者在发生错误时返回空字符串。

函数 parse_web_content 解析网页内容，接收网页链接和HTML内容，返回一个包含链接、标题、提取码、获取链接的字典。

函数 fetch_baidu_link 获取百度链接，接收包含链接信息的字典，返回一个包含百度链接和提取码的字符串，或者在发生错误时返回 None。

函数 write_results 将结果写入文件，接收包含要写入的结果的列表和输出文件的路径，如果写入成功返回True，否则返回False。

这个模块主要用于网页信息的抓取和解析，并存储结果。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import concurrent.futures
import logging
import os
import random
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple, Any, Optional

import requests
from lxml import etree
from retrying import retry

from my_module import read_json_to_dict

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

# 初始化配置
CONFIG = read_json_to_dict('config/scrapy_game_1.json')
BASE_URL = CONFIG['scrapy_game_1']['base_url']  # 网站域名
START_NUMBER = CONFIG['scrapy_game_1']['start_number']  # 开始计数
STOP_NUMBER = CONFIG['scrapy_game_1']['stop_number']  # 停止计数
OUTPUT_TXT = CONFIG['scrapy_game_1']['output_txt']  # 每个结果写 4 行
USER_COOKIE = CONFIG['scrapy_game_1']['user_cookie']  # 帐号 cookie
THREAD_NUMBER = CONFIG['scrapy_game_1']['thread_number']  # 线程数
PROXIES_LIST = CONFIG['scrapy_game_1']['proxies_list']  # 代理池
REQUEST_HEAD = CONFIG['scrapy_game_1']['request_head']  # 请求标头，不含帐号 cookie
REQUEST_HEAD["Cookie"] = USER_COOKIE  # 请求标头，更新帐号 cookie


def handle_result(result: Any, link: str) -> None:
    """
    处理结果并进行存储。

    :type result: Any
    :param result: 结果对象
    :param link: 网页链接
    :type link: str
    :rtype: None
    :return: 无返回值
    """
    try:
        write_results([result])
    except Exception as e:
        logger.error(f"链接：{link} 在写入结果时发生错误: {e}")


def error_callback(e: Exception, link: str) -> None:
    """
    处理进程中的错误。

    :type e: Exception
    :param e: 发生的错误对象
    :param link: 网页链接
    :type link: str
    :rtype: None
    :return: 无返回值
    """
    logger.error(f"链接：{link} 在处理进程中发生错误: {e}")


def scrapy_game_1(base_url: str = BASE_URL, start_number: int = START_NUMBER, stop_number: int = STOP_NUMBER, thread_number: int = THREAD_NUMBER) -> None:
    """
    对指定链接进行处理。

    :type base_url: str
    :param base_url: 网站域名
    :type start_number: int
    :param start_number: 启动计数
    :type stop_number: int
    :param stop_number: 停止计数
    :type thread_number: int
    :param thread_number: 线程数
    :rtype: None
    :return: 无返回值
    """
    failed = threading.Lock()
    failed_count = 0

    try:
        links = [f"{base_url}/game/{item}.html" for item in [str(i) for i in range(start_number + 1, stop_number + 1)]]
    except Exception as e:
        logger.error(f"生成链接列表发生错误：{e}")
        return

    try:
        with ThreadPoolExecutor(max_workers=thread_number) as executor:
            futures = {executor.submit(main, link): link for link in links}
            for future in concurrent.futures.as_completed(futures):
                link = futures[future]
                try:
                    result = future.result()
                    if any(not item for item in result):
                        with failed:
                            failed_count += 1
                    handle_result(result, link)
                except Exception as e:
                    error_callback(e, link)
    except Exception as e:
        logger.error(f"链接：{link} 在分配线程时发生错误：{e}")
    finally:
        logger.info(f"总计数量：{stop_number - start_number}，失败数量：{failed_count}")


def main(link: str) -> Tuple[str, str, Optional[str]]:
    """
    爬虫主流程。

    :param link: 网页链接
    :type link: str
    :rtype: Tuple[str, str, Optional[str]]
    :return: 链接, 标题, 百度链接带提取码
    """
    try:
        fetch_web_response = fetch_web_page(link)
        if not fetch_web_response:
            return link, '', ''

        parse_web_result = parse_web_content(link, fetch_web_response)
        if not parse_web_result['fetched_link']:
            return link, '', ''

        baidu_link = fetch_baidu_link(parse_web_result)
        if not baidu_link:
            return link, parse_web_result["title"], ''

        return parse_web_result["link"], parse_web_result["title"], baidu_link
    except Exception as e:
        logger.error(f"链接：{link} 获取下载时运行错误: {e}")
        return link, '', ''


@retry(stop_max_attempt_number=10, wait_random_min=100, wait_random_max=1200)
def fetch_web_page(link: str) -> Optional[str]:
    """
    获取网页HTML内容

    :param link: 要访问的网页链接
    :type link: str
    :rtype: Optional[str]
    :return: 网页的HTML内容，或者在发生错误时返回空字符串
    """
    try:
        proxy_server = random.choice(PROXIES_LIST)
        response = requests.get(link, headers=REQUEST_HEAD, timeout=15, verify=False, allow_redirects=False, proxies=proxy_server)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.error(f"链接：{link} 请求失败: {e}")
        return ''


def parse_web_content(link: str, html: str) -> Dict[str, str]:
    """
    解析网页内容

    :param link: 网页的链接
    :type link: str
    :param html: 网页的HTML内容
    :type html: str
    :rtype: Dict[str, str]
    :return: 一个字典，包含链接，标题，提取码，获取链接
    """
    try:
        response_etree = etree.HTML(html)
        download_info = re.findall(r'<a href="javascript:;" class="downbtn normal"(.+)百度网盘</a>', html)
        if not download_info:
            logger.error(f'链接：{link} 获取百度网盘下载信息失败，请手动检查页面')
            return {'link': link, 'title': '', 'password': '', 'fetched_link': ''}
        title = response_etree.xpath('//div[@class="article-tit"]/h1/text()')[0].strip()
        download_info_new = download_info[0]
        password = re.findall(r'data-info=\'(.*)\'', download_info_new)[0]
        fetched_link = re.findall(r'data-url="(.+)"><i', download_info_new)[0]
        return {'link': link, 'title': title, 'password': password, 'fetched_link': fetched_link}
    except Exception as e:
        logger.error(f"链接：{link} 解析HTML内容失败: {e}")
        return {'link': link, 'title': '', 'password': '', 'fetched_link': ''}


@retry(stop_max_attempt_number=3, wait_random_min=100, wait_random_max=1200)
def fetch_baidu_link(fetch_web_response: Dict[str, str], base_url: str = BASE_URL) -> Optional[str]:
    """
    获取百度链接。

    :param base_url: 网站域名
    :type base_url: str
    :param fetch_web_response: 一个包含链接信息的字典。
    :type fetch_web_response: Dict[str, str]
    :rtype: Optional[str]
    :return: 一个字符串，包含百度链接和提取码，或者在发生错误时返回 None。
    """
    try:
        proxy_server = random.choice(PROXIES_LIST)
        link = f'{base_url}{fetch_web_response["fetched_link"]}'
        response = requests.get(link, headers=REQUEST_HEAD, timeout=15, verify=False, allow_redirects=False, proxies=proxy_server)
        response.raise_for_status()
        baidu_link = response.headers.get('location')
        if baidu_link:
            return f'{baidu_link} {fetch_web_response["password"]}'
        else:
            logger.error(f'链接：{fetch_web_response["link"]} 没获取到百度下载链接，帐号问题？')
            return ''

    except Exception as e:
        logger.error(f'链接：{fetch_web_response["link"]} 获取百度下载链接时发生错误：{e}')
        return ''


def write_results(results: List[Tuple[str, str, Optional[str]]], output_file: str = OUTPUT_TXT) -> bool:
    """
    将结果写入文件。

    :param results: 包含要写入的结果的列表，列表中的每个元素都是一个包含链接、标题和百度链接代码的元组。
    :type results: List[Tuple[str, str, Optional[str]]]
    :param output_file: 输出文件的路径。
    :type output_file: str
    :rtype: bool
    :return: 如果写入成功返回True，否则返回False。
    """
    try:
        output_file = os.path.normpath(output_file)
        with open(output_file, "a", encoding='utf-8') as file:
            for link, title, baidu_link_code in results:
                file.write(f'{link}\n{title}\n{baidu_link_code}\n{"*" * 52}\n')
                logger.info(f'成功抓取：{link}, {title}, {baidu_link_code}') if title else None
        return True
    except Exception as e:
        logger.error(f"写入结果时发生错误：{e}")
        return False
