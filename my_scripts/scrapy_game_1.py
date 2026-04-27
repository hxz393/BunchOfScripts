# coding=utf-8
"""
抓取游戏页面标题和百度网盘跳转地址，并顺序写入文本结果。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import concurrent.futures
import logging

import os
import random
import re
from retrying import retry
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple

import requests
from lxml import etree

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
ResultRow = Tuple[str, str, str]


def handle_result(result: ResultRow, link: str) -> None:
    """
    将单条抓取结果追加写入输出文件。

    :param result: 一条抓取结果，格式为 ``(链接, 标题, 百度链接+提取码)``。
    :type result: ResultRow
    :param link: 网页链接
    :type link: str
    :return: 无
    """
    try:
        write_results([result])
    except Exception:
        logger.exception(f"链接：{link} 在写入结果时发生错误")


def scrapy_game_1() -> None:
    """
    并发抓取配置范围内的游戏页面。

    单页抓取失败只计入失败数，不会中断整批任务。

    :return: 无
    """
    failed_count = 0

    try:
        links = [f"{BASE_URL}/game/{item}.html" for item in range(START_NUMBER + 1, STOP_NUMBER + 1)]
        with ThreadPoolExecutor(max_workers=THREAD_NUMBER) as executor:
            futures = {executor.submit(main, link): link for link in links}
            for future in concurrent.futures.as_completed(futures):
                link = futures[future]
                try:
                    result = future.result()
                    if any(not item for item in result):
                        failed_count += 1
                        continue
                    handle_result(result, link)
                except Exception:
                    failed_count += 1
                    logger.exception(f"链接：{link} 在处理进程中发生错误")
    except Exception:
        logger.exception("分配线程时发生错误")
    finally:
        logger.info(f"总计数量：{STOP_NUMBER - START_NUMBER}，失败数量：{failed_count}")


def main(link: str) -> ResultRow:
    """
    爬虫主流程。

    :param link: 网页链接
    :type link: str
    :return: 链接, 标题, 百度链接带提取码
    :rtype: ResultRow
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
    except Exception:
        logger.exception(f"链接：{link} 获取下载时运行错误")
        return link, '', ''


@retry(stop_max_attempt_number=3, wait_random_min=100, wait_random_max=1200)
def fetch_web_page(link: str) -> str:
    """
    获取网页 HTML 内容。

    :param link: 要访问的网页链接
    :type link: str
    :return: 网页 HTML 内容
    :rtype: str
    """
    proxy_server = random.choice(PROXIES_LIST)
    response = requests.get(link, headers=REQUEST_HEAD, timeout=15, verify=False, allow_redirects=False, proxies=proxy_server)
    response.raise_for_status()
    return response.text


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
    except Exception:
        logger.exception(f"链接：{link} 解析HTML内容失败")
        return {'link': link, 'title': '', 'password': '', 'fetched_link': ''}


# @retry(stop_max_attempt_number=5, wait_random_min=100, wait_random_max=1200)
def fetch_baidu_link(fetch_web_response: Dict[str, str], base_url: str = BASE_URL) -> str:
    """
    获取百度网盘跳转地址，并拼上提取码。

    :param base_url: 网站域名
    :type base_url: str
    :param fetch_web_response: 一个包含链接信息的字典。
    :type fetch_web_response: Dict[str, str]
    :return: 百度链接与提取码；如果页面没有跳转地址则返回空字符串
    :rtype: str
    """
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


def write_results(results: List[ResultRow], output_file: str = OUTPUT_TXT) -> bool:
    """
    将结果写入文件。

    :param results: 要写入的结果列表。
    :type results: List[ResultRow]
    :param output_file: 输出文件路径。
    :type output_file: str
    :return: 写入成功返回 ``True``，否则返回 ``False``。
    :rtype: bool
    """
    try:
        output_file = os.path.normpath(output_file)
        with open(output_file, "a", encoding='utf-8') as file:
            for link, title, baidu_link_code in results:
                file.write(f'{link}\n{title}\n{baidu_link_code}\n{"*" * 52}\n')
                if title:
                    logger.info(f'完成抓取：{link}, {title}, {baidu_link_code}')
        return True
    except Exception:
        logger.exception("写入结果时发生错误")
        return False
