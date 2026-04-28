"""
已停用的 IMDb 网页抓取解析逻辑。

这些代码来自 2026-04-27 前后的 ``sort_movie.py``、``sort_movie_director.py``
和 ``sort_movie_request.py``。当前主流程已经改为本地 IMDb 镜像查询；
这里仅保留旧版网页抓取和 ``__NEXT_DATA__`` 解析逻辑，供查阅或临时回滚参考。
"""
import json
import logging
import sys
import time
from typing import Any, Optional

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from retrying import retry

from my_module import read_json_to_dict

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG = read_json_to_dict("config/sort_movie_request.json")

IMDB_MOVIE_URL = CONFIG["imdb_movie_url"]
IMDB_PERSON_URL = CONFIG["imdb_person_url"]
IMDB_HEADER = CONFIG["imdb_header"]
IMDB_COOKIE = CONFIG["imdb_cookie"]
IMDB_HEADER["Cookie"] = IMDB_COOKIE

_cached_cookie = None
_last_cookie_time = 0


def safe_get(d: dict, path: list, default: Any = None) -> Any:
    """
    根据 path 列表，从字典 d 中逐层安全获取值。
    如果任意一步为 None 或不是 dict，就返回 default。

    :param d: 查询字典
    :param path: 查询键列表
    :param default: 出现问题时返回的默认值
    :return: 读取到的值；失败时返回 ``default``
    """
    for key in path:
        if not isinstance(d, dict):
            return default
        d = d.get(key)
        if d is None:
            return default
    return d


@retry(stop_max_attempt_number=5, wait_random_min=2420, wait_random_max=3700)
def get_imdb_cookie(force=False, hl=False):
    """用浏览器访问 IMDB 刷新 Cookie。"""
    global _last_cookie_time, _cached_cookie
    now = time.time()

    if force:
        _cached_cookie = None
    elif _cached_cookie is not None and (now - _last_cookie_time) < 300:
        return _cached_cookie

    logger.warning("更新 IMDB Cookie")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=hl,
            channel="chrome",
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()
        page.goto("https://www.imdb.com/title/tt0759924/", wait_until="domcontentloaded", timeout=60000)

        # cookie 准备往往早于搜索框渲染。
        page.wait_for_timeout(15000 if hl else 30000)

        cookies = context.cookies()
        cookie_names = {c["name"] for c in cookies}

        if "session-id" not in cookie_names:
            title = page.title()
            html = page.content()
            browser.close()

            if "challenge.js" in html or "challenge-container" in html or "403 Forbidden" in title:
                raise RuntimeError("IMDb 当前会拦截这个会话，headless 刷 cookie 不可靠")
            raise RuntimeError(f"IMDb cookie 未就绪，当前只有: {sorted(cookie_names)}")

        cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
        browser.close()

    _cached_cookie = cookie_str
    _last_cookie_time = time.time()
    return cookie_str


@retry(
    stop_max_attempt_number=50,
    wait_random_min=300,
    wait_random_max=3000,
    retry_on_exception=lambda e: isinstance(e, requests.RequestException),
)
def get_imdb_movie_response(movie_id: str) -> Optional[requests.Response]:
    """
    从 IMDB 获取电影信息，返回结果供解析。

    :param movie_id: 电影 imdb 编号
    :return: 成功时返回响应
    """
    logger.info(f"查询 IMDB：{movie_id}")
    url = f"{IMDB_MOVIE_URL}/{movie_id}/"
    cookie_dict = get_imdb_cookie()
    IMDB_HEADER["Cookie"] = cookie_dict
    response = requests.get(url, timeout=15, verify=False, allow_redirects=False, headers=IMDB_HEADER)
    if response.status_code != 200:
        logger.error(f"IMDB 访问失败！状态码：{response.status_code}")
        sys.exit(f"被墙了 {response.status_code}：{url}")
    return response


@retry(
    stop_max_attempt_number=50,
    wait_random_min=300,
    wait_random_max=3000,
    retry_on_exception=lambda e: isinstance(e, requests.RequestException),
)
def get_imdb_director_response(director_id: str) -> Optional[requests.Response]:
    """
    从 IMDB 获取导演信息，返回结果供解析。

    :param director_id: 导演 imdb 编号
    :return: 成功时返回响应
    """
    url = f"{IMDB_PERSON_URL}/{director_id}/"
    cookie_dict = get_imdb_cookie(force=True, hl=False)
    IMDB_HEADER["Cookie"] = cookie_dict
    response = requests.get(url, timeout=15, verify=False, allow_redirects=False, headers=IMDB_HEADER)
    if response.status_code != 200:
        logger.error(f"IMDB 访问失败！状态码：{response.status_code}")
        sys.exit(f"被墙了 {response.status_code}：{url}")
    return response


def get_imdb_movie_details(movie_id) -> Optional[dict]:
    """
    解析 IMDB 页面中的 ``__NEXT_DATA__`` JSON 数据。

    :param movie_id: 电影 imdb 编号
    :return: 成功时返回解析出来的 JSON
    """
    response = get_imdb_movie_response(movie_id)
    if not response:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    script_tag = soup.find("script", id="__NEXT_DATA__")
    if not script_tag:
        logger.error("IMDB 页面未找到 id 为 __NEXT_DATA__ 的 script 标签")
        return None

    json_text = script_tag.string.strip()
    try:
        return json.loads(json_text)
    except json.JSONDecodeError as e:
        logger.error("IMDB JSON 解析失败:", e)
        return None


def get_imdb_movie_info(movie_id: str, movie_info: dict) -> None:
    """
    从 IMDB 网页 JSON 获取电影信息，储存到传入的字典中。

    :param movie_id: 电影 imdb 编号
    :param movie_info: 电影信息字典，原地修改
    :return: 无
    """
    m = get_imdb_movie_details(movie_id)
    if not m:
        logger.error(f"imdb 解析失败！{movie_id}")
        return

    if not movie_info["year"]:
        year = safe_get(m, ["props", "pageProps", "aboveTheFoldData", "releaseYear", "year"], default=0)
        movie_info["year"] = year

    runtime = safe_get(m, ["props", "pageProps", "aboveTheFoldData", "runtime", "seconds"], default=0)
    runtime_imdb = int(runtime / 60)
    if not movie_info["runtime"]:
        movie_info["runtime"] = runtime_imdb
    movie_info["runtime_imdb"] = runtime_imdb

    original_title = safe_get(m, ["props", "pageProps", "aboveTheFoldData", "originalTitleText", "text"], default="")
    movie_info["titles"].append(original_title)
    if not movie_info["original_title"]:
        movie_info["original_title"] = original_title

    aka_edges = safe_get(m, ["props", "pageProps", "mainColumnData", "akas", "edges"], default=[])
    first_edges_item = aka_edges[0] if aka_edges else {}
    aka_title = safe_get(first_edges_item, ["node", "text"], default="")
    if aka_title:
        movie_info["titles"].append(aka_title)

    genre_texts = [
        safe_get(item, ["genre", "text"], default="")
        for item in safe_get(m, ["props", "pageProps", "aboveTheFoldData", "titleGenres", "genres"], default=[])
    ]
    movie_info["genres"].extend(genre_texts)

    country_ids = [
        safe_get(item, ["id"], default="")
        for item in safe_get(m, ["props", "pageProps", "aboveTheFoldData", "countriesOfOrigin", "countries"], default=[])
    ]
    movie_info["country"].extend(country_ids)

    languages = [
        safe_get(item, ["id"], default="")
        for item in safe_get(m, ["props", "pageProps", "mainColumnData", "spokenLanguages", "spokenLanguages"], default=[])
    ]
    movie_info["language"].extend(languages)

    principal_credits = safe_get(
        m,
        ["props", "pageProps", "aboveTheFoldData", "principalCreditsV2"],
        default=[],
    )
    directors_list = []
    for group in principal_credits:
        if safe_get(group, ["grouping", "text"]) in {"Director", "Directors"}:
            for credit in safe_get(group, ["credits"], default=[]):
                director = safe_get(credit, ["name", "nameText", "text"])
                if director:
                    directors_list.append(director)
            break
    movie_info["directors"].extend(directors_list)


def get_imdb_director_info(director_id: str) -> dict[str, list]:
    """
    从 IMDB 人物页获取导演信息。

    :param director_id: 导演 imdb 编号
    :return: 返回一个字典，包含别名和国别
    """
    director_info = {"country": [], "aka": []}
    response = get_imdb_director_response(director_id)
    if not response:
        return director_info

    soup = BeautifulSoup(response.text, "html.parser")
    title_tag = soup.title
    if title_tag:
        director_info["aka"].append(title_tag.string.replace(" - IMDb", ""))

    birth_place_tag = soup.find("a", href=lambda x: x and "ref_=nm_pdt_bth_loc" in x)
    if birth_place_tag:
        director_info["country"].append(birth_place_tag.get_text(strip=True))

    return director_info
