"""
抓取 hde 站点发布信息

配置文件 ``config/scrapy_hde.json`` 需要提供：
- ``hde_url``: 站点根地址。
- ``output_dir``: 生成 ``.rls`` 文件的输出目录。
- ``end_titles``: 分页抓取时用于判断“已经追到旧数据”的截止标题列表。
  正常跑完一轮后，脚本会把首次访问页的前两个标题写回这里，供下次运行使用。
- ``max_workers``: 详情页并发抓取线程数。
- ``default_release_size``: 标题里提取不到体积信息时使用的默认值。
- ``request_timeout_seconds``: 单次 HTTP 请求超时秒数。
- ``retry_max_attempts`` / ``retry_wait_min_ms`` / ``retry_wait_max_ms``:
  ``get_hde_response`` 的重试参数。

主流程：
1. 抓取列表页并解析出标题、详情页链接和体积。
2. 并发访问详情页，提取 IMDb 编号并落盘为 ``.rls`` 文件。
3. 当列表页中出现任一截止标题时停止翻页。
4. 只有整轮成功结束后，才把首次访问页的前两个标题写回配置文件。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from retrying import retry

from my_module import normalize_release_title_for_filename, read_json_to_dict, sanitize_filename, update_json_config, write_list_to_file

logger = logging.getLogger(__name__)


CONFIG_PATH = 'config/scrapy_hde.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # 配置文件

HDE_URL = CONFIG['hde_url']  # hde 地址
OUTPUT_DIR = CONFIG['output_dir']  # 输出目录
END_TITLES = [title.strip() for title in CONFIG['end_titles'] if isinstance(title, str) and title.strip()]
DEFAULT_MAX_WORKERS = CONFIG['max_workers']
DEFAULT_RELEASE_SIZE = CONFIG['default_release_size']
REQUEST_TIMEOUT_SECONDS = CONFIG['request_timeout_seconds']
RETRY_MAX_ATTEMPTS = CONFIG['retry_max_attempts']
RETRY_WAIT_MIN_MS = CONFIG['retry_wait_min_ms']
RETRY_WAIT_MAX_MS = CONFIG['retry_wait_max_ms']
END_TITLES_KEEP_COUNT = 2

SIZE_WITH_DASH_PATTERN = re.compile(r"\s[–-]\s*([\d.]+\s*(?:GB|MB|TB))\s*$")
TRAILING_SIZE_PATTERN = re.compile(r"([\d.]+\s*(?:GB|MB|TB))\s*$")
IMDB_URL_PATTERN = re.compile(r"https?://(?:www\.)?imdb\.com/title/(tt\d+)")
IMDB_ID_PATTERN = re.compile(r"(tt\d+)")
IMAGE_LINK_PATTERN = re.compile(r"\.(?:png|jpe?g|gif|webp|avif)(?:$|[?#])", re.IGNORECASE)


def build_hde_page_url(page_number: int) -> str:
    """根据页码构造 HDE 电影列表页 URL。"""
    return f"{HDE_URL}tag/movies/page/{page_number}/"


def should_stop_scrapy(result_list: List[Dict[str, str]], end_titles: List[str]) -> bool:
    """当前批次命中任一截止标题时返回 True。"""
    if not end_titles:
        return False

    return any(result_item.get("title") in end_titles for result_item in result_list)


def select_next_end_titles(result_list: List[Dict[str, str]], keep_count: int = END_TITLES_KEEP_COUNT) -> List[str]:
    """从首次访问页中选出下一轮要写回配置的前几个标题。"""
    titles = [result_item.get("title", "").strip() for result_item in result_list if result_item.get("title", "").strip()]
    return titles[:keep_count]


def scrapy_hde(start_page: int = 1) -> None:
    """
    从 ``start_page`` 开始连续抓取，直到命中配置中的任一截止标题为止。

    每一页会先解析列表，再并发访问详情页写出 ``.rls`` 文件。
    首次访问页的前两个标题会先记在内存里，整轮成功结束后再写回配置。
    """
    if not END_TITLES:
        raise ValueError("至少需要提供一个截止标题")

    next_end_titles: List[str] | None = None

    logger.info("抓取 hde 站点发布信息")
    while True:
        logger.info(f"抓取第 {start_page} 页")
        url = build_hde_page_url(start_page)
        response = get_hde_response(url)
        result_list = parse_hde_response(response)
        logger.info(f"共 {len(result_list)} 个结果")

        if next_end_titles is None:
            next_end_titles = select_next_end_titles(result_list)

        if not process_all(result_list, max_workers=DEFAULT_MAX_WORKERS):
            raise RuntimeError("HDE 详情页抓取存在失败，已停止且未更新截止标题配置")

        # 检查日期
        if should_stop_scrapy(result_list, END_TITLES):
            logger.info("没有新发布，完成")
            break

        logger.warning("-" * 255)
        start_page += 1

    if next_end_titles:
        update_json_config(CONFIG_PATH, "end_titles", next_end_titles)


def process_all(result_list, max_workers=5):
    """并发处理一批详情页任务；有失败时返回 ``False``，供主流程决定是否终止。"""
    has_error = False
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {
            executor.submit(visit_hde_url, item): item
            for item in result_list
        }
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                future.result()
            except Exception as exc:
                has_error = True
                logger.error(f"[ERROR] {item} -> {exc!r}")
    return not has_error


@retry(
    stop_max_attempt_number=RETRY_MAX_ATTEMPTS,
    wait_random_min=RETRY_WAIT_MIN_MS,
    wait_random_max=RETRY_WAIT_MAX_MS,
)
def get_hde_response(url: str, session: requests.Session | None = None) -> requests.Response:
    """请求页面并统一做编码设置与状态码校验。"""
    request_client = session or requests
    response = request_client.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.encoding = 'utf-8'
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    return response


@retry(
    stop_max_attempt_number=RETRY_MAX_ATTEMPTS,
    wait_random_min=RETRY_WAIT_MIN_MS,
    wait_random_max=RETRY_WAIT_MAX_MS,
)
def post_hde_response(url: str, payload: List[tuple[str, str]], session: requests.Session) -> requests.Response:
    """提交内容保护表单并返回解锁后的页面。"""
    response = session.post(
        url,
        data=payload,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.encoding = 'utf-8'
    if response.status_code != 200:
        raise Exception(f"请求失败，重试 {response.status_code}：{url}")

    return response


def parse_hde_response(response: requests.Response) -> list:
    """解析 HDE 单页列表，输出 ``title/url/size`` 字典列表。"""
    soup = BeautifulSoup(response.text, "lxml")
    results = []
    for fit in soup.select("div.fit.item"):
        result_item = parse_hde_item(fit)
        if result_item:
            results.append(result_item)
    return results


def parse_hde_item(fit) -> Dict[str, str] | None:
    """解析单个列表条目，缺少必要节点时返回 ``None``。"""
    data_div = fit.select_one("div.data")
    if not data_div:
        return None

    a_tag = data_div.select_one("h5 a")
    if not a_tag:
        return None

    title = a_tag.get_text(strip=True)
    url = a_tag.get("href", "").strip()
    return {
        "title": title,
        "url": url,
        "size": extract_release_size(title),
    }


def extract_release_size(title: str, default_size: str = DEFAULT_RELEASE_SIZE) -> str:
    """从标题末尾提取体积信息，失败时回退到配置中的默认体积。"""
    match = SIZE_WITH_DASH_PATTERN.search(title)
    if not match:
        match = TRAILING_SIZE_PATTERN.search(title)
    if match:
        return match.group(1).replace(" ", "")
    return default_size.replace(" ", "")


def visit_hde_url(result_item: dict):
    """
    访问详情页并写出对应的 ``.rls`` 文件。
    """
    url = result_item["url"]
    logger.info(f"访问 {url}")
    session = requests.Session()
    response = get_hde_response(url, session=session)
    soup = BeautifulSoup(response.text, 'lxml')
    result_item["imdb"] = extract_imdb_id_from_soup(soup)
    soup = unlock_hde_protected_soup(url, soup, session)
    content = build_hde_output_content(url, result_item["imdb"], soup)
    path = os.path.join(OUTPUT_DIR, build_hde_output_filename(result_item))
    write_list_to_file(path, content)


def find_hde_protected_form(soup: BeautifulSoup):
    """从详情页中定位内容保护表单。"""
    for form in soup.find_all("form"):
        if form.find("input", attrs={"name": "content-protector-token"}) is not None:
            return form
    return None


def build_hde_protected_form_payload(form) -> List[tuple[str, str]]:
    """提取内容保护表单里需要回发的字段。"""
    payload = []
    for input_tag in form.find_all("input"):
        input_type = input_tag.get("type", "").lower()
        name = input_tag.get("name", "").strip()
        if not name:
            continue
        if input_type in {"hidden", "submit"}:
            payload.append((name, input_tag.get("value", "")))
    return payload


def unlock_hde_protected_soup(url: str, soup: BeautifulSoup, session: requests.Session) -> BeautifulSoup:
    """如果页面带内容保护表单，则自动提交表单并返回解锁后的页面。"""
    protected_form = find_hde_protected_form(soup)
    if protected_form is None:
        return soup

    payload = build_hde_protected_form_payload(protected_form)
    if not payload:
        return soup

    response = post_hde_response(url, payload, session)
    return BeautifulSoup(response.text, "lxml")


def build_hde_output_content(url: str, imdb_id: str, soup: BeautifulSoup) -> List[str]:
    """生成写入 ``.rls`` 文件的内容：详情页地址、IMDb 地址和解锁后的下载链接。"""
    useful_links = extract_hde_useful_links(soup)
    if imdb_id:
        useful_links = dedupe_hde_links([f"https://www.imdb.com/title/{imdb_id}/", *useful_links])
    return [url, *useful_links]


def extract_hde_useful_links(soup: BeautifulSoup) -> List[str]:
    """从正文中提取 IMDb 和解锁后的下载链接，过滤截图等无用链接。"""
    entry_content = soup.select_one("div.entry-content") or soup
    links = []
    for anchor in entry_content.find_all("a", href=True):
        href = urljoin(HDE_URL, anchor["href"].strip())
        if is_hde_useful_link(href):
            links.append(href)
    return dedupe_hde_links(links)


def is_hde_useful_link(href: str) -> bool:
    """判断链接是否值得写入输出文件。"""
    if not href:
        return False

    lower_href = href.lower()
    if "/cdn-cgi/l/email-protection" in lower_href:
        return False
    if IMAGE_LINK_PATTERN.search(lower_href) or "pixhost." in lower_href:
        return False
    if "imdb.com/title/" in lower_href or lower_href.startswith("magnet:"):
        return True

    parsed = urlparse(href)
    if parsed.netloc and parsed.netloc not in {"hdencode.org", "www.hdencode.org"}:
        return True
    return False


def dedupe_hde_links(links: Iterable[str]) -> List[str]:
    """按原顺序去重链接列表。"""
    deduped = []
    seen = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        deduped.append(link)
    return deduped


def extract_imdb_id_from_soup(soup: BeautifulSoup) -> str:
    """从详情页所有链接中提取 IMDb 编号。"""
    return extract_imdb_id_from_links(a["href"] for a in soup.find_all("a", href=True))


def extract_imdb_id_from_links(hrefs: Iterable[str]) -> str:
    """
    优先从标准 IMDb 标题页 URL 提取，其次回退到宽松 ``tt`` 编号匹配。
    """
    fallback_imdb_id = ""
    for href in hrefs:
        match = IMDB_URL_PATTERN.search(href)
        if match:
            return match.group(1)

        if not fallback_imdb_id:
            match = IMDB_ID_PATTERN.search(href)
            if match:
                fallback_imdb_id = match.group(1)

    return fallback_imdb_id


def build_hde_output_filename(result_item: Dict[str, str]) -> str:
    """根据发布信息拼装输出文件名，格式为 ``标题 - hde (体积)[IMDb].rls``。"""
    file_name = normalize_release_title_for_filename(result_item["title"])
    file_name = sanitize_filename(file_name)
    return f"{file_name} - hde ({result_item['size']})[{result_item.get('imdb', '')}].rls"
