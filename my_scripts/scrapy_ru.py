"""
抓取 ru 站点指定栏目的所有帖子。
从帖子列表下载种子？

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from lxml import etree
from retrying import retry

from my_module import read_json_to_dict, sanitize_filename, update_json_config

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

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


def parse_topic_row(row) -> dict[str, str] | None:
    """
    从帖子行中提取标题、ID、帖子链接、大小和下载链接。

    :param row: 单条帖子对应的 ``tr`` 节点
    :return: 成功时返回解析结果，缺少关键字段时返回 ``None``
    """
    # 找标题及链接
    title_element = row.xpath('.//td[@class="vf-col-t-title tt"]//a[contains(@class, "torTopic bold tt-text")]')
    if not title_element:
        return None

    # 取第一个匹配到的 <a> 标签
    a_tag = title_element[0]
    title_text = a_tag.xpath('string(.)').strip()  # 帖子标题
    topic_href = a_tag.xpath('@href')[0]  # 类似 "viewtopic.php?t=6375868"
    topic_id = topic_href.split('t=', 1)[1]  # 纯 ID "6375868"
    topic_link = f"{FORUM_URL}{topic_href}"  # 拼凑出完整链接

    # 找文件大小和下载地址
    dl_element = row.xpath('.//td[@class="vf-col-tor tCenter med nowrap"]//a[@class="small f-dl dl-stub"]')
    if not dl_element:
        return None

    dl_tag = dl_element[0]
    size_text = dl_tag.xpath('string(.)').strip()  # 例如 "272.1 MB"
    dl_href = dl_tag.xpath('@href')[0]  # 例如 "dl.php?t=6375868"
    download_link = f"{FORUM_URL}{dl_href}"

    return {
        "title_text": title_text,
        "topic_id": topic_id,
        "topic_link": topic_link,
        "size_text": size_text,
        "download_link": download_link,
    }


def build_output_filename(title_text: str, topic_id: str, size_text: str) -> str:
    """
    根据标题、帖子 ID 和大小生成输出文件名。

    :param title_text: 帖子标题
    :param topic_id: 帖子 ID
    :param size_text: 页面展示的大小文本
    :return: 处理后的输出文件名
    """
    if len(title_text) > 228:
        title_text = title_text[:228]

    file_name = f"{title_text}[{topic_id}][{size_text}].txt"
    file_name = file_name.replace("/", "｜").replace("\\", "｜")
    return sanitize_filename(file_name)


def get_next_page_url(tree) -> str | None:
    """
    从当前页面中提取下一页完整链接。

    :param tree: 当前页面的 HTML 树
    :return: 存在下一页时返回完整 URL，否则返回 ``None``
    """
    next_link = tree.xpath('//a[@class="pg" and text()="След."]')
    if not next_link:
        return None

    return f"{FORUM_URL}{next_link[0].get('href')}"


def write_topic_file(file_name: str, file_content: str) -> None:
    """
    将帖子内容写入到目标目录中的文本文件。

    :param file_name: 输出文件名
    :param file_content: 文件内容
    :return: 无
    """
    with open(os.path.join(TORRENT_PATH, file_name), "w", encoding='utf-8') as file:
        file.write(file_content)


def process_page_rows(row_elements, stop_id: int, current_max_id: str | None) -> tuple[bool, str | None]:
    """
    处理单页中的所有帖子行，返回是否需要停止翻页及更新后的最大帖子 ID。

    :param row_elements: 当前页的帖子行列表
    :param stop_id: 本栏目上次已记录的最大帖子 ID
    :param current_max_id: 目前为止已抓到的最大帖子 ID
    :return: ``(stop, max_id)``
    """
    stop = False
    new_max_id = current_max_id

    for row in row_elements:
        topic_info = parse_topic_row(row)
        if topic_info is None:
            continue
        title_text = topic_info["title_text"]
        topic_id = topic_info["topic_id"]
        topic_link = topic_info["topic_link"]
        size_text = topic_info["size_text"]
        download_link = topic_info["download_link"]

        # 拼凑出文件名，由帖子标题+ID+大小组成
        file_name = build_output_filename(title_text, topic_id, size_text)
        # 文件内容，为帖子地址和种子下载链接，共两行
        file_content = f"{topic_link}\n{download_link}"

        # 计算是否中断，小于最后记录则跳过写文件
        if int(topic_id) < stop_id:
            stop = True
        else:
            # 写入到本地文本文件
            write_topic_file(file_name, file_content)
            if new_max_id is None or int(topic_id) > int(new_max_id):
                new_max_id = topic_id

    return stop, new_max_id


def scripy(url: str) -> None:
    """
    抓取所有链接，并写入到文件。

    :param url: 栏目地址，例如："https://rutracker.org/forum/viewforum.php?f=106"
    :return: 无
    """
    base_url = url.split('&sort=2')[0]
    stop_id = int(CONFIG['scrapy_process'].get(base_url, 0))
    current_url = url
    new_max_id = None

    while current_url:
        # 请求一页内容
        r = get_page(current_url)

        # 找到所有种子行，以 class="hl-tr" 为标记
        tree = etree.HTML(r.text)
        row_elements = tree.xpath('//tr[@class="hl-tr"]')
        if not row_elements:
            raise RuntimeError(f"链接：{current_url} 未找到种子行")

        stop, new_max_id = process_page_rows(row_elements, stop_id, new_max_id)

        if stop:
            break

        next_page_url = get_next_page_url(tree)
        if not next_page_url:
            break

        logger.info(f"开始下一页链接: {next_page_url}")
        current_url = next_page_url

    if new_max_id is not None:
        update_json_config(CONFIG_PATH, ["scrapy_process", base_url], new_max_id)
