"""
抓取 rlsbb 站点发布信息

配置文件 ``config/scrapy_rls.json`` 需要提供：
- ``rls_url``: 站点根地址。
- ``output_dir``: 生成 ``.rls`` 文件的输出目录。
- ``foreign_end_titles``: ``f_mode=True`` 时的截止标题列表。
- ``movie_end_titles``: ``f_mode=False`` 时的截止标题列表。
  正常跑完一轮后，脚本会把首次访问页的前两个标题写回当前流程对应的配置键。

主流程：
1. 公开入口会依次抓取 ``foreign-movies`` 和 ``movies`` 两条流程。
2. 每条流程都按各自分类列表页抓取。
3. 并发访问详情页，提取 IMDb 编号并落盘为 ``.rls`` 文件。
4. 当列表页中出现当前流程配置里的任一截止标题时停止翻页。
5. 只有整轮成功结束后，才把首次访问页的前两个标题写回当前流程对应的配置键。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2025, hxz393. 保留所有权利。
"""
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

import requests
from bs4 import BeautifulSoup
from retrying import retry

from my_module import normalize_release_title_for_filename, read_json_to_dict, sanitize_filename, update_json_config, write_list_to_file

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG_PATH = 'config/scrapy_rls.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

RLS_URL = CONFIG['rls_url']  # rlsbb 地址
RLS_COOKIE = CONFIG['rls_cookie']  # 用户甜甜
REQUEST_HEAD = CONFIG['request_head']  # 请求头
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录
MAX_WORKERS = CONFIG.get("max_workers", 40)
END_TITLES_KEEP_COUNT = 2

REQUEST_HEAD["Cookie"] = RLS_COOKIE  # 请求头加入认证


def normalize_rls_title(title: str) -> str:
    """统一截止标题和列表页标题的比较格式。"""
    return title.strip().replace(" ⭐", "").replace(" ", ".")


def build_rls_page_url(page_number: int, f_mode: bool = True) -> str:
    """根据页码和抓取模式构造 RLS 列表页 URL。"""
    category = "foreign-movies" if f_mode else "movies"
    return f"{RLS_URL}category/{category}/page/{page_number}/?s="


def get_end_titles_key(f_mode: bool = True) -> str:
    """返回当前抓取模式对应的截止标题配置键。"""
    return "foreign_end_titles" if f_mode else "movie_end_titles"


def get_current_end_titles(f_mode: bool = True) -> List[str]:
    """读取当前模式对应的截止标题列表，并做统一格式清洗。"""
    config = read_json_to_dict(CONFIG_PATH)
    raw_end_titles = config.get(get_end_titles_key(f_mode), [])
    return [
        normalize_rls_title(title)
        for title in raw_end_titles
        if isinstance(title, str) and title.strip()
    ]


def should_stop_scrapy(result_list: List[Dict[str, str]], end_titles: List[str]) -> bool:
    """当前批次命中任一截止标题时返回 ``True``。"""
    if not end_titles:
        return False

    return any(result_item.get("title") in end_titles for result_item in result_list)


def select_next_end_titles(result_list: List[Dict[str, str]], keep_count: int = END_TITLES_KEEP_COUNT) -> List[str]:
    """从首次访问页中选出下一轮要写回配置的前几个标题。"""
    titles = [result_item.get("title", "").strip() for result_item in result_list if result_item.get("title", "").strip()]
    return titles[:keep_count]


def _scrapy_rls_single_mode(start_page: int = 1, f_mode: bool = True) -> None:
    """执行单个分类流程的抓取与截止标题更新。"""
    end_titles = get_current_end_titles(f_mode)
    if not end_titles:
        raise ValueError("至少需要提供一个截止标题")

    next_end_titles: List[str] | None = None
    end_titles_key = get_end_titles_key(f_mode)

    logger.info("抓取 rlsbb 站点发布信息")
    while True:
        logger.info(f"抓取第 {start_page} 页")
        url = build_rls_page_url(start_page, f_mode)
        while True:
            response = get_rls_response(url)
            result_list = parse_rls_response(response)
            if len(result_list):
                break
            time.sleep(3)
            logger.warning("等待 3 秒后重试")
        logger.info(f"共 {len(result_list)} 个结果")

        if next_end_titles is None:
            next_end_titles = select_next_end_titles(result_list)

        if not process_all(result_list, max_workers=MAX_WORKERS):
            raise RuntimeError("RLS 详情页抓取存在失败，已停止且未更新截止标题配置")

        # 终止检查
        if should_stop_scrapy(result_list, end_titles):
            logger.info("没有新发布，完成")
            break

        logger.warning("-" * 255)
        start_page += 1

    if next_end_titles:
        update_json_config(CONFIG_PATH, end_titles_key, next_end_titles)


def scrapy_rls(start_page: int = 1) -> None:
    """按固定顺序依次抓取外语电影和普通电影两条流程。"""
    logger.info("抓取 rlsbb 站点发布信息：foreign-movies")
    _scrapy_rls_single_mode(start_page=start_page, f_mode=True)
    logger.warning("-" * 255)
    logger.info("抓取 rlsbb 站点发布信息：movies")
    _scrapy_rls_single_mode(start_page=start_page, f_mode=False)


def process_all(result_list, max_workers=5):
    """
    并发调用 ``visit_rls_url``，result_list 中每个元素都会被提交到线程池执行。
    max_workers 控制并发线程数，视网络 I/O 或目标服务器承受能力调整。
    """
    has_error = False
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_item = {
            executor.submit(visit_rls_url, item): item
            for item in result_list
        }
        # 按完成顺序收集结果或捕获异常
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                future.result()
            except Exception as exc:
                has_error = True
                logger.error(f"[ERROR] {item} -> {exc!r}")
    return not has_error


@retry(stop_max_attempt_number=150, wait_random_min=1000, wait_random_max=10000)
def get_rls_response(url: str) -> requests.Response:
    """请求流程"""
    response = requests.get(url, timeout=35, headers=REQUEST_HEAD)
    response.encoding = 'utf-8'
    if response.status_code == 403:
        sys.exit(f"被墙了 {response.status_code}：{url}")
    elif response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    return response


def parse_rls_response(response: requests.Response) -> list:
    """解析响应文本"""
    soup = BeautifulSoup(response.text, "html.parser")
    results = []
    # 找到所有 class="p-c p-c‑title" 的 div
    for title_div in soup.find_all("div", class_="p-c p-c-title"):
        a_tag = title_div.find("h2").find("a")
        title_text = a_tag.get_text(strip=True)
        url = a_tag.get("href")
        # 替换无关符号
        title_text = normalize_rls_title(title_text)

        # # 用正则提取括号里面的大小 (比如 "394MB" 或 "1.45GB")
        # m = re.search(r"\(([\d.]+\s*(?:GB|MB|TB))\)", title_text)
        # if m:
        #     size = m.group(1)
        # else:
        #     size = "100GB"  # 如果没匹配到，就设默认值

        results.append({
            "title": title_text,
            "url": url
        })

    return results


def visit_rls_url(result_item: dict):
    """访问详情页"""
    url = result_item["url"]
    logger.info(f"访问 {url}")
    response = get_rls_response(url)
    soup = BeautifulSoup(response.text, 'lxml')

    imdb_id = ""

    # 找所有 a 标签，href 包含 /title/tt
    for a in soup.find_all('a', href=True):
        href = a['href']
        # 查找 imdb title 链接
        m = re.search(r"https?://(?:www\.)?imdb\.com/title/(tt\d+)", href)
        if m:
            imdb_id = m.group(1)
            break

    # 如果找不到，可以尝试更宽松匹配
    if not imdb_id:
        for a in soup.find_all('a', href=True):
            href = a['href']
            m2 = re.search(r"(tt\d+)", href)
            if m2:
                imdb_id = m2.group(1)
                break

    # 存回 result_item 或返回
    result_item["imdb"] = imdb_id
    file_name = normalize_release_title_for_filename(result_item['title'])
    file_name = sanitize_filename(file_name)
    file_name = f"{file_name} - rls [{imdb_id}].rls"
    path = os.path.join(OUTPUT_DIR, file_name)
    write_list_to_file(path, [url])
