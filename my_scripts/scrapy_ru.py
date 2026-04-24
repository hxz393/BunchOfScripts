"""
抓取 ru 站点指定栏目的所有帖子。
从帖子列表下载种子？

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from lxml import etree
from retrying import retry

from my_module import read_json_to_dict, sanitize_filename

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()
CONFIG_LOCK = threading.Lock()

CONFIG_PATH = 'config/scrapy_ru.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

SCRAPY_GROUP = CONFIG['scrapy_process'].keys()  # 栏目地址列表
USER_COOKIE = CONFIG['user_cookie']  # 用户甜甜
REQUEST_HEAD = CONFIG['request_head']  # 请求头
THREAD_NUMBER = CONFIG['thread_number']  # 线程数
TORRENT_PATH = CONFIG['torrent_path']  # 种子保存目录
FORUM_URL = CONFIG['forum_url']  # 种子保存目录

REQUEST_HEAD["Cookie"] = USER_COOKIE  # 请求头加入认证


def scrapy_ru() -> None:
    """
    多线程抓取所有链接，并写入到文件。
    """
    with ThreadPoolExecutor(max_workers=THREAD_NUMBER) as executor:
        future_to_url = {executor.submit(scripy, url): url for url in SCRAPY_GROUP}
        # 等待所有任务完成
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                future.result()
                logger.info(f"抓取完成：{url}")
            except Exception as e:
                logger.error(f"抓取出错：{url}，错误：{e}")


def update_json_config(file_path: str, key: str, new_value: str) -> None:
    """
    更新 JSON 配置文件中的某个键的值。

    :param file_path: JSON 配置路径
    :param key: 键
    :param new_value: 值
    :return: 无
    """
    temp_file_path = f"{file_path}.tmp"
    with CONFIG_LOCK:
        with open(file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)  # config 是个 dict

        config["scrapy_process"][key] = new_value

        with open(temp_file_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)

        os.replace(temp_file_path, file_path)


@retry(stop_max_attempt_number=10, wait_random_min=100, wait_random_max=1200)
def get_page(url: str) -> requests.Response:
    """
    请求单页内容，失败时自动重试。

    :param url: 栏目地址
    :return: 响应对象
    """
    r = requests.get(url=f"{url}&sort=2", headers=REQUEST_HEAD, timeout=10, verify=False, allow_redirects=True)
    if r.status_code != 200:
        raise RuntimeError(f"链接：{url} 无法访问，状态码：{r.status_code}")
    return r


def scripy(url: str, group_url: str | None = None) -> None:
    """
    抓取所有链接，并写入到文件。

    :param url: 栏目地址，例如："https://rutracker.org/forum/viewforum.php?f=106"
    :param group_url: 原始栏目地址，用于翻页时持续读取和更新同一个配置项
    :return: 无
    """
    base_url = group_url or url.split('&sort=2')[0]

    # 请求一页内容
    r = get_page(url)

    # 找到所有种子行，以 class="hl-tr" 为标记
    tree = etree.HTML(r.text)
    row_elements = tree.xpath('//tr[@class="hl-tr"]')
    if not row_elements:
        raise RuntimeError(f"链接：{url} 未找到种子行")

    # 遍历每一行
    stop = False
    stop_id = CONFIG['scrapy_process'].get(base_url, 0)
    new_max_id = None
    for row in row_elements:
        # 找标题及链接
        title_element = row.xpath('.//td[@class="vf-col-t-title tt"]//a[contains(@class, "torTopic bold tt-text")]')
        if not title_element:
            continue
        # 取第一个匹配到的 <a> 标签
        a_tag = title_element[0]
        title_text = a_tag.xpath('string(.)').strip()  # 帖子标题
        topic_href = a_tag.xpath('@href')[0]  # 类似 "viewtopic.php?t=6375868"
        topic_id = topic_href.split('t=', 1)[1]  # 纯 ID "6375868"
        topic_link = f"{FORUM_URL}{topic_href}"  # 拼凑出完整链接

        # 找文件大小和下载地址
        dl_element = row.xpath('.//td[@class="vf-col-tor tCenter med nowrap"]//a[@class="small f-dl dl-stub"]')
        if not dl_element:
            continue
        dl_tag = dl_element[0]
        size_text = dl_tag.xpath('string(.)').strip()  # 例如 "272.1 MB"
        dl_href = dl_tag.xpath('@href')[0]  # 例如 "dl.php?t=6375868"
        download_link = f"{FORUM_URL}{dl_href}"

        # 拼凑出文件名，由帖子标题+ID+大小组成
        if len(title_text) > 228:
            title_text = title_text[:228]
        file_name = f"{title_text}[{topic_id}][{size_text}].txt"
        file_name = file_name.replace("/", "｜").replace("\\", "｜")
        file_name = sanitize_filename(file_name)
        # 文件内容，为帖子地址和种子下载链接，共两行
        file_content = f"{topic_link}\n{download_link}"

        # 计算是否中断，小于最后记录则跳过写文件
        if int(topic_id) < int(stop_id):
            stop = True
        else:
            # 写入到本地文本文件
            with open(os.path.join(TORRENT_PATH, file_name), "w", encoding='utf-8') as file:
                file.write(file_content)
            if new_max_id is None or int(topic_id) > int(new_max_id):
                new_max_id = topic_id

    # 查找下一页链接
    next_link = tree.xpath('//a[@class="pg" and text()="След."]')
    if next_link and not stop:
        # 如果能找到此链接，说明还有下一页
        href_value = f"{FORUM_URL}{next_link[0].get('href')}"
        logger.info(f"开始下一页链接: {href_value}")
        scripy(href_value, base_url)

    # 更新本地配置
    if new_max_id is not None:
        update_json_config(CONFIG_PATH, base_url, new_max_id)
