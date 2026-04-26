"""
抓取 bds 站点发布信息

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import datetime
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from retrying import retry
from urllib3.util.retry import Retry

from my_module import read_json_to_dict, sanitize_filename, update_json_config
from scrapy_redis import get_redis_client

CONFIG_PATH = 'config/scrapy_bds.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

GROUP_DICT = CONFIG['group_dict']  # 栏目 ID 字典
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录
BDS_URL = CONFIG['bds_url']  # BDS 站点地址
BDS_COOKIE = CONFIG['bds_cookie']  # 用户甜甜
REQUEST_HEAD = dict(CONFIG['request_head'])  # 请求头
END_TIME = CONFIG.get('end_time', '2020-09-21')  # 截止日期
REDIS_SEEN_KEY = CONFIG.get('redis_seen_key', 'bds_seen')  # 已抓取 URL 集合
MAX_WORKERS = 6  # 详情页抓取并发数

REQUEST_HEAD["Cookie"] = BDS_COOKIE  # 请求头加入认证
START_URL = BDS_URL + "forum.php?mod=forumdisplay&fid={fid}&page={page}"

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()


def create_retry_strategy() -> Retry:
    """兼容 urllib3 1.x / 2.x 的 Retry 初始化参数。"""
    retry_kwargs = {
        "total": 15,
        "status_forcelist": [502],
        "backoff_factor": 1,
    }
    try:
        return Retry(allowed_methods=["POST", "GET"], **retry_kwargs)
    except TypeError:
        return Retry(method_whitelist=["POST", "GET"], **retry_kwargs)


retry_strategy = create_retry_strategy()
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=8, pool_maxsize=16)
session = requests.Session()
session.proxies = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890", }
session.mount("http://", adapter)
session.mount("https://", adapter)


def get_previous_day(date_str: str) -> str:
    """返回 ``date_str`` 的前一天，格式保持 ``YYYY-mm-dd``。"""
    date_value = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    previous_day = date_value - datetime.timedelta(days=1)
    return previous_day.strftime("%Y-%m-%d")


def get_current_end_time() -> str:
    """读取当前配置中的截止日期，避免同进程多次运行时使用过期值。"""
    return read_json_to_dict(CONFIG_PATH).get("end_time", END_TIME)


def parse_bds_date(date_str: str) -> datetime.datetime | None:
    """尝试把页面日期解析为 ``datetime``。"""
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d")
    except (TypeError, ValueError):
        return None


def get_yesterday_date_str(reference_date: datetime.date | None = None) -> str:
    """返回当天前一天的日期字符串，格式为 ``YYYY-mm-dd``。"""
    if reference_date is None:
        reference_date = datetime.date.today()
    return get_previous_day(reference_date.strftime("%Y-%m-%d"))


def finalize_bds_run(had_failures: bool) -> None:
    """在本轮抓取完成后回写配置中的 ``end_time``。"""
    if had_failures:
        logger.warning("BDS 有详情任务失败，暂不回写 end_time")
        return

    next_end_time = get_yesterday_date_str()
    update_json_config(CONFIG_PATH, "end_time", next_end_time)
    logger.info(f"BDS 已更新 end_time 为 {next_end_time}")


def append_page_results(all_results: list[dict], seen_links: set[str], page_results: list[dict]) -> bool:
    """按 URL 合并单页结果；遇到重复 URL 时返回 ``True`` 表示停止翻页。"""
    for item in page_results:
        link = item["link"]
        if link in seen_links:
            return True
        seen_links.add(link)
        all_results.append(item)
    return False


def collect_group_results(
        group_name: str,
        group_id: int,
        start_page: int,
        stop_date: datetime.datetime,
) -> list[dict]:
    """顺序抓取单个栏目，并按 URL 去重。"""
    all_results = []
    seen_links = set()
    current_page = start_page

    while True:
        logger.info(f"爬取栏目 {group_name} ，爬取页面 {current_page} …")
        results, stop = parse_forum_page(group_id, current_page, stop_date)
        if not results:
            logger.info(f"栏目 {group_name} 没有爬取结果")
            break
        logger.info(f"共 {len(results)} 个帖子")

        has_duplicate_link = append_page_results(all_results, seen_links, results)
        if stop or has_duplicate_link:
            break

        current_page += 1

    return all_results


def filter_seen_items(all_results: list[dict], redis_client=None) -> list[dict]:
    """过滤掉 Redis seen 集合中已存在的 URL。"""
    if not all_results:
        return []

    if redis_client is None:
        redis_client = get_redis_client()

    pipe = redis_client.pipeline()
    for item in all_results:
        pipe.sismember(REDIS_SEEN_KEY, item["link"])
    seen_results = pipe.execute()

    unseen_items = []
    seen_count = 0
    for already_seen, item in zip(seen_results, all_results):
        if already_seen:
            seen_count += 1
            continue
        unseen_items.append(item)

    if seen_count:
        logger.info(f"BDS seen 集合已过滤 {seen_count} 条已抓取 URL")

    return unseen_items


def mark_seen_url(url: str, redis_client=None) -> None:
    """将成功写出的帖子 URL 记录到 Redis seen 集合。"""
    if redis_client is None:
        redis_client = get_redis_client()
    redis_client.sadd(REDIS_SEEN_KEY, url)


def build_bds_output_filename(title: str, tt: str) -> str:
    """生成输出文件名。"""
    safe_title = re.sub(r'[\\/:*?"<>|]', " ", title)
    safe_title = sanitize_filename(safe_title).strip()
    return f"{safe_title}[{tt}].bds"


def scrapy_bds(start_page: int = 1, end_time: str | None = None) -> None:
    """
    抓取新发布内容写入到文件。
    """
    logger.info("抓取 bds 站点发布信息")
    if end_time is None:
        end_time = get_current_end_time()

    redis_client = get_redis_client()
    stop_date = datetime.datetime.strptime(end_time, "%Y-%m-%d")
    had_failures = False
    for group_name, group_id in GROUP_DICT.items():
        all_results = collect_group_results(group_name, group_id, start_page, stop_date)
        unseen_results = filter_seen_items(all_results, redis_client=redis_client)
        logger.info("-" * 255)
        logger.info(f"总共 {len(all_results)} 个帖子，待抓取 {len(unseen_results)} 个")
        process_results = process_all(unseen_results, max_workers=MAX_WORKERS, redis_client=redis_client)
        if len(process_results) != len(unseen_results):
            had_failures = True
        logger.info("-" * 255)

    finalize_bds_run(had_failures)


def process_all(all_results: list[dict], max_workers: int = 5, redis_client=None) -> list[str]:
    """多线程访问链接"""
    if not all_results:
        return []

    if redis_client is None:
        redis_client = get_redis_client()

    results: list[str] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_item = {
            executor.submit(read_thread, item, redis_client): item
            for item in all_results
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


def read_thread(item: dict, redis_client=None) -> str:
    """ 在帖子内获取 tt 编号 """
    link = f"{item['link']}&_dsign=39e16b34"
    resp = get_bds_response(link)
    match = re.search(r"tt\d+", resp.text)
    tt = match.group(0) if match else ""
    if not tt:
        logger.warning(f"没有找到 tt 编号：{link}")

    filename = build_bds_output_filename(item["title"], tt)
    file_path = os.path.join(OUTPUT_DIR, filename)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(item["link"])
    mark_seen_url(item["link"], redis_client=redis_client)
    time.sleep(0.05)
    return item["link"]


def parse_forum_tbody(tbody) -> dict | None:
    """解析单个帖子块，缺少必要节点时返回 ``None``。"""
    th = tbody.find("th")
    if not th:
        return None

    anchor = th.find("a", class_="s xst")
    if not anchor:
        return None

    href = anchor.get("href")
    if not href:
        raise RuntimeError("BDS 列表项缺少 href，整页抓取已中止")

    title = anchor.get_text(strip=True)
    full_link = urljoin(BDS_URL, href)
    td_by = tbody.find("td", class_="by")
    time_span = td_by.find("span") if td_by else None
    date_str = time_span.get_text(strip=True) if time_span else ""
    return {
        "title": title,
        "link": full_link,
        "date": date_str,
    }


def parse_forum_page(group_id: int, start_page: int, stop_time: datetime.datetime) -> tuple[list[dict], bool]:
    """ 获取某个栏目的所有帖子 """
    url = START_URL.format(fid=group_id, page=start_page)
    resp = get_bds_response(url)
    soup = BeautifulSoup(resp.text, "html.parser")

    table = soup.find("table", {"id": "threadlisttableid"})
    if table is None:
        logger.error("没有找到帖子！样式更新了？")
        return [], False

    result_list = []
    stop = False

    for tbody in table.find_all("tbody"):
        item = parse_forum_tbody(tbody)
        if item is None:
            continue

        post_date = parse_bds_date(item["date"])
        if post_date is None or post_date >= stop_time:
            result_list.append(item)
        elif start_page != 1:
            stop = True

    return result_list, stop


@retry(stop_max_attempt_number=15, wait_random_min=15000, wait_random_max=20000)
def get_bds_response(url: str) -> requests.Response:
    """请求流程"""
    logger.info(f"访问 {url}")
    response = session.get(url, timeout=30, verify=False, headers=REQUEST_HEAD)
    response.encoding = 'utf-8'
    if response.status_code != 200:
        logger.error(f"请求失败，重试 {response.status_code}：{url}")
        raise Exception(f"请求失败")

    if len(response.text) < 10000:
        logger.error(f"请求被封锁，重试：{url}\n{response.text}")
        raise Exception(f"请求被封锁")

    return response
