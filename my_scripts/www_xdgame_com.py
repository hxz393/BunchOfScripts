# coding=utf-8
import logging
import os
import random
import re
from multiprocessing import Pool, freeze_support
from typing import Dict, List, Tuple, Any, Optional, Union

import requests
from lxml import etree
from retrying import retry

from my_module import config_read, read_file_to_list

logger = logging.getLogger(__name__)
# noinspection PyUnresolvedReferences
requests.packages.urllib3.disable_warnings()

# 初始化配置
CP = config_read('config/config.ini')
INPUT_TXT = CP.get('www_xdgame_com', 'input_txt')  # 一行一个链接
OUTPUT_TXT = CP.get('www_xdgame_com', 'output_txt')  # 每个结果写 4 行
USER_COOKIE = CP.get('www_xdgame_com', 'user_cookie')  # 帐号 cookie
PROCESS_NUMBER = CP.getint('www_xdgame_com', 'process_number')  # 进程数

REQUEST_HEAD = {
    'Host': 'www.xdgame.com',
    'Connection': 'keep-alive',
    'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-User': '?1',
    'Sec-Fetch-Dest': 'document',
    'Referer': 'https://www.xdgame.com',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7,en-GB;q=0.6,ru;q=0.5',
    'Cache-Control': 'max-age=0',
    'Cookie': USER_COOKIE
}
PROXIES_LIST = [
    # {"http": "http://192.168.2.102:808", "https": "http://192.168.2.102:808"},
    {"http": "http://192.168.2.204:8888", "https": "http://192.168.2.204:8888"},
]


def handle_result(result: Any) -> None:
    """
    处理结果并进行存储。

    :type result: Any
    :param result: 结果对象
    :rtype: None
    :return: 无返回值
    """
    try:
        write_results([result])
    except Exception as e:
        logger.error(f"在处理结果时发生错误: {e}")


def error_callback(e: Exception) -> None:
    """
    处理进程中的错误。

    :type e: Exception
    :param e: 发生的错误对象
    :rtype: None
    :return: 无返回值
    """
    logger.error(f"在进程中发生错误: {e}")


def www_xdgame_com(input_txt: Union[str, os.PathLike] = INPUT_TXT, process_number: int = PROCESS_NUMBER) -> None:
    """
    对指定文件中的链接进行处理。

    :type input_txt: Union[str, os.PathLike]
    :param input_txt: 包含链接的输入文件
    :type process_number: int
    :param process_number: 进程数
    :rtype: None
    :return: 无返回值
    """
    try:
        links = read_file_to_list(input_txt)
    except Exception as e:
        logger.error(f"读取输入文件发生错误：{e}")
        return

    try:
        with Pool(processes=process_number) as pool:
            for link in links:
                pool.apply_async(main, args=(link,), callback=handle_result, error_callback=error_callback)
            pool.close()
            pool.join()
    except Exception as e:
        logger.error(f"分配进程时发生错误：{e}")


def main(link: str) -> Tuple[str, str, Optional[str]]:
    """
    主程序。

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

        logger.info(f'成功抓取：{link}, {parse_web_result["title"]}, {baidu_link}')
        return parse_web_result["link"], parse_web_result["title"], baidu_link
    except Exception as e:
        logger.error(f"链接：{link} 获取下载时运行错误: {e}")
        return link, '', ''


@retry(stop_max_attempt_number=100, wait_random_min=100, wait_random_max=1200)
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


@retry(stop_max_attempt_number=2, wait_random_min=100, wait_random_max=1200)
def fetch_baidu_link(fetch_web_response: Dict[str, str]) -> Optional[str]:
    """
    获取百度链接。

    :param fetch_web_response: 一个包含链接信息的字典。
    :type fetch_web_response: Dict[str, str]
    :rtype: Optional[str]
    :return: 一个字符串，包含百度链接和提取码，或者在发生错误时返回 None。
    """
    try:
        proxy_server = random.choice(PROXIES_LIST)
        link = f'https://www.xdgame.com{fetch_web_response["fetched_link"]}'
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
        return True
    except Exception as e:
        logger.error(f"写入结果时发生错误：{e}")
        return False


if __name__ == '__main__':
    freeze_support()
    try:
        www_xdgame_com()
    except Exception as error:
        logger.error(error)
