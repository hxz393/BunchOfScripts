"""
此模块提供了整理MZ博客下载文件的功能，主要包括从mz博客中提取信息、下载图片和移动文件。

主要功能集中在三个函数：`sort_mz`、`get_files_info_and_move` 和 `download_and_save_pictures`。这些函数分别负责处理博客文章的不同方面，例如解析页面、下载图片和整理文件。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""

import logging
import os
import re
import shutil
from typing import Dict, Optional, List

import requests
from lxml import etree
from requests import Response
from retrying import retry

from my_module import read_json_to_dict, read_file_to_list, write_list_to_file

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()


def sort_mz(source_dir: str,
            target_dir: str) -> None:
    """
    整理mz博客的下载文件。

    :param source_dir: 整理来源文件目录。
    :type source_dir: str
    :param target_dir: 整理好后存放目录。
    :type target_dir: str

    :rtype: None
    :return: 无返回。
    """
    # 读取配置文件
    config = read_json_to_dict('config/sort_mz.json')
    unsupported_str = config['sort_mz']['unsupported_str']  # 非法字符
    url_file = config['sort_mz']['url_file']  # 地址列表文件
    url_list = read_file_to_list(url_file)  # 地址列表
    file_dict = {filename: os.path.join(source_dir, filename) for filename in os.listdir(source_dir)}  # 文件信息字典

    # 遍历URL列表，逐条开始处理
    for url in url_list.copy():
        try:
            logger.info(f"开始处理链接：{url}")
            response_mz = request_url(url)
            if response_mz is None:
                logger.error(f"链接：{url} 无法访问")
                continue

            # 先提取mz页面中信息
            content_mz = etree.HTML(response_mz.text)
            title_mz = content_mz.xpath('//*[@id="Blog1"]/div[1]/div/div/div/div[1]/h3/text()')[0]
            title_mz = title_mz.replace('\n', '').rstrip('.').strip()
            title_mz = re.sub(unsupported_str, "_", title_mz)
            logger.debug(f'得到文章标题：{title_mz}')
            target_path = os.path.join(target_dir, title_mz)
            picture_urls = content_mz.xpath('//div[@class="post-body entry-content"]/div/a/@href')
            logger.debug(f'得到图片地址：{picture_urls}')
            gd_urls = content_mz.xpath("//a[contains(@href, 'drive.google.com')]/@href")
            logger.debug(f'得到谷歌网盘地址：{gd_urls}')
            if not gd_urls:
                logger.warning(f"链接：{url} 中没有谷歌网盘地址，跳过。")
                continue

            # 根据谷歌网盘地址获取文件名，然后移动文件
            get_files_info_and_move(gd_urls, file_dict, target_path)
            # 一张一张下载图片
            download_and_save_pictures(picture_urls, target_path)
            # 链接处理完毕后，从列表中移除URL，并更新文件
            url_list.remove(url)
            write_list_to_file(url_file, url_list)
            logger.info(f"链接处理完成：{url}\n")
        except Exception:
            logger.exception(f"发生错误: {url}")


def get_files_info_and_move(gd_urls: List[str],
                            file_dict: Dict[str, str],
                            target_path: str) -> None:
    """
    访问Google Drive下载链接，获取文件信息，并将已下载的文件移动到指定目录。

    :param gd_urls: Google Drive下载链接列表。
    :type gd_urls: List[str]
    :param file_dict: 要整理的文件名与文件路径的映射字典。
    :type file_dict: Dict[str, str]
    :param target_path: 目标路径，用于存放整理好的文件。
    :type target_path: str

    :rtype: None
    :return: 无返回值。
    """
    # 访问谷歌网盘地址，可能有多个。
    for gd_url in gd_urls:
        try:
            logger.debug(f"开始访问谷歌网盘链接：{gd_url}")
            response_gd = request_url(gd_url)
            if response_gd is None:
                logger.error(f"谷歌网盘链接：{gd_url} 无法访问")
                continue

            # 再提取gd页面中信息
            content_gd = etree.HTML(response_gd.text)
            title_gd = content_gd.xpath("//meta[@property='og:title']/@content")[0]
            logger.debug(f'得到网盘文件名：{title_gd}')
            # 检查网盘文件名存不存在本地
            if title_gd not in file_dict:
                logger.warning(f"在本地没找到文件：{title_gd}")
                continue

            # 最后移动压缩文件
            os.makedirs(target_path, exist_ok=True)
            source_path = file_dict[title_gd]
            shutil.move(source_path, target_path)
            logger.debug(f"文件: {source_path} 移动到 {target_path}")
        except Exception:
            logger.exception(f"移动整理文件时发生错误: {target_path}")


def download_and_save_pictures(picture_urls: List[str],
                               target_path: str) -> None:
    """
    下载和保存博文中的图片到指定位置。

    :param picture_urls: 要下载的图片链接列表。
    :type picture_urls: List[str]
    :param target_path: 下载保存位置。
    :type target_path: str

    :rtype: None
    :return: 无返回。
    """
    for picture_url in picture_urls:
        try:
            logger.debug(f"开始下载图片：{picture_url}")
            # 访问图片失败时跳过
            response_picture = request_url(picture_url)
            if response_picture is None:
                logger.error(f"图片下载失败：{picture_url}")
                continue

            # 写入到文件中
            picture_path = os.path.join(target_path, os.path.basename(picture_url))
            with open(picture_path, 'wb') as f:
                f.write(response_picture.content)
                logger.debug(f'图片下载到：{picture_path}')
        except Exception:
            logger.exception(f"下载图片时发生错误: {picture_url}")


@retry(stop_max_attempt_number=3, wait_random_min=30, wait_random_max=300)
def request_url(url: str) -> Optional[Response]:
    """
    向指定的URL发送请求并返回响应内容。

    此函数会尝试发送HTTP GET请求到提供的URL，并在成功时返回一个 `Response` 对象。如果请求失败，将自动重试最多3次。

    :param url: 要请求的URL地址。
    :type url: str

    :rtype: Optional[Response]
    :return: 请求成功时返回响应对象，否则返回 None。
    """
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response
