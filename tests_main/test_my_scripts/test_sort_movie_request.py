"""
针对 ``my_scripts.sort_movie_request`` 的定向单元测试。

这里只验证当前修复点：
1. ``get_csfd_movie_details`` 只应在导演标题块中提取导演。
2. 既支持捷克语 ``Režie``，也支持英语 ``Directed`` 标题。
3. ``get_douban_search_details`` 只在结果足够明确时返回唯一目标链接。
"""

import copy
import importlib.util
import sys
import tempfile
import types
import urllib.parse
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

requests = __import__("requests")
requests.packages.urllib3.disable_warnings()

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "sort_movie_request.py"


def load_sort_movie_request(config: dict | None = None):
    """
    在隔离环境中加载 ``sort_movie_request`` 模块。

    被测模块在 import 时会读取配置并依赖多个第三方模块，
    这里注入最小桩实现，只保留本测试需要的 ``get_csfd_movie_details``。
    """
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "tmdb_url": "https://example.com/tmdb",
        "tmdb_auth": "token",
        "tmdb_key": "key",
        "tmdb_image_url": "https://example.com/image",
        "imdb_movie_url": "https://example.com/imdb/title",
        "imdb_person_url": "https://example.com/imdb/name",
        "imdb_header": {"User-Agent": "unit-test"},
        "imdb_cookie": "imdb_cookie=value",
        "douban_header": {"User-Agent": "unit-test"},
        "douban_cookie": "douban_cookie=value",
        "douban_movie_url": "https://example.com/douban/movie",
        "douban_person_url": "https://example.com/douban/person",
        "douban_search_url": "https://example.com/douban/search",
        "csfd_header": {"User-Agent": "unit-test"},
        "csfd_cookie": "csfd_cookie=value",
        "kpk_search_url": "https://example.com/kpk/search",
        "kpk_page_url": "https://example.com/kpk/page",
        "kpk_header": {"User-Agent": "unit-test"},
        "jackett_search_url": "https://example.com/jackett/search",
        "jackett_api_key": "jackett-key",
    }
    if config:
        module_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: copy.deepcopy(module_config)

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    fake_xmltodict = types.ModuleType("xmltodict")
    fake_xmltodict.parse = lambda *_args, **_kwargs: {}

    fake_tmdbv3api = types.ModuleType("tmdbv3api")

    class DummyTMDb:
        def __init__(self):
            self.api_key = None

    class DummyMovie:
        def details(self, *_args, **_kwargs):
            return {}

        def alternative_titles(self, *_args, **_kwargs):
            return {}

    class DummyTV(DummyMovie):
        pass

    class DummyPerson:
        def details(self, *_args, **_kwargs):
            return {}

        def movie_credits(self, *_args, **_kwargs):
            return {}

    fake_tmdbv3api.TMDb = DummyTMDb
    fake_tmdbv3api.Movie = DummyMovie
    fake_tmdbv3api.TV = DummyTV
    fake_tmdbv3api.Person = DummyPerson

    fake_tmdbv3api_as_obj = types.ModuleType("tmdbv3api.as_obj")

    class DummyAsObj(dict):
        pass

    fake_tmdbv3api_as_obj.AsObj = DummyAsObj

    fake_tmdbv3api_exceptions = types.ModuleType("tmdbv3api.exceptions")
    fake_tmdbv3api_exceptions.TMDbException = Exception

    fake_playwright = types.ModuleType("playwright")
    fake_playwright_sync_api = types.ModuleType("playwright.sync_api")
    fake_playwright_sync_api.sync_playwright = lambda: None

    spec = importlib.util.spec_from_file_location(
        f"sort_movie_request_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "retrying": fake_retrying,
            "xmltodict": fake_xmltodict,
            "tmdbv3api": fake_tmdbv3api,
            "tmdbv3api.as_obj": fake_tmdbv3api_as_obj,
            "tmdbv3api.exceptions": fake_tmdbv3api_exceptions,
            "playwright": fake_playwright,
            "playwright.sync_api": fake_playwright_sync_api,
        },
    ):
        spec.loader.exec_module(module)

    return module, temp_dir


def build_csfd_html(
        creator_blocks: list[tuple[str, str]],
        imdb_href: str | None = None,
        same_as_href: str = "https://www.csfd.cz/film/102778",
        origin: str = "USA, 2024, 120 min",
) -> str:
    """构造最小可用的 CSFD 页面 HTML。"""
    creators_html = "".join(
        f'<div><h4>{heading}</h4><a href="/creator/{index}">{name}</a></div>'
        for index, (heading, name) in enumerate(creator_blocks, start=1)
    )
    imdb_html = f'<a class="button-imdb" href="{imdb_href}">IMDb</a>' if imdb_href else ""
    same_as_html = f'<a itemprop="sameAs" href="{same_as_href}">CSFD</a>' if same_as_href else ""
    return (
        f'<div class="origin">{origin}</div>'
        f'<div id="creators">{creators_html}</div>'
        f"{imdb_html}"
        f"{same_as_html}"
    )


def build_douban_search_html(results: list[tuple[str, str]]) -> str:
    """构造最小可用的豆瓣搜索结果 HTML。"""
    items_html = "".join(
        (
            '<div class="result">'
            f'<div class="pic"><a title="{title}">Poster</a></div>'
            f'<a class="nbg" href="https://search.douban.com/movie/subject_search?url={urllib.parse.quote(target_url, safe="")}">Link</a>'
            '</div>'
        )
        for title, target_url in results
    )
    return (
        '<div class="search-result"></div>'
        f'<div class="result-list">{items_html}</div>'
    )


class TestGetCsfdMovieDetails(unittest.TestCase):
    """验证 CSFD 页面解析逻辑中的导演块匹配。"""

    def setUp(self):
        self.module, self.temp_dir = load_sort_movie_request()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_csfd_movie_details_uses_rezie_block_instead_of_first_creator_block(self):
        """前面出现演员块时，不应误把演员名当导演。"""
        response = Mock(
            text=build_csfd_html(
                creator_blocks=[
                    ("Hrají:", "Actor Name"),
                    ("Režie:", "Director Name"),
                ],
                imdb_href="https://www.imdb.com/title/tt1234567/",
            )
        )

        result = self.module.get_csfd_movie_details(response)

        self.assertEqual(
            result,
            {
                "origin": "USA, 2024, 120 min",
                "director": "Director Name",
                "id": "tt1234567",
            },
        )

    def test_get_csfd_movie_details_accepts_directed_heading_and_csfd_fallback_id(self):
        """英语 ``Directed`` 标题也应命中，并在无 IMDb 时回退到 CSFD ID。"""
        response = Mock(
            text=build_csfd_html(
                creator_blocks=[
                    ("Writers:", "Writer Name"),
                    ("Directed by:", "Director Name"),
                ],
                imdb_href=None,
                same_as_href="https://www.csfd.cz/film/654321/",
            )
        )

        result = self.module.get_csfd_movie_details(response)

        self.assertEqual(
            result,
            {
                "origin": "USA, 2024, 120 min",
                "director": "Director Name",
                "id": "csfd654321",
            },
        )


class TestGetDoubanSearchDetails(unittest.TestCase):
    """验证豆瓣搜索结果只在足够明确时返回唯一链接。"""

    def setUp(self):
        self.module, self.temp_dir = load_sort_movie_request()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_douban_search_details_skips_known_noise_first_result(self):
        """遇到已知噪声项时，应返回第二条真实目标链接。"""
        response = Mock(
            text=build_douban_search_html(
                [
                    ("It's Hard to be Nice", "https://movie.douban.com/subject/1111111/"),
                    ("Real Movie", "https://movie.douban.com/subject/2222222/"),
                ]
            )
        )

        result = self.module.get_douban_search_details(response)

        self.assertEqual(result, "https://movie.douban.com/subject/2222222/")


if __name__ == "__main__":
    unittest.main()
