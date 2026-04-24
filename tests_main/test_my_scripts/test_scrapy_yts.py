"""
针对 ``my_scripts.scrapy_yts`` 的单元测试。

这里不依赖真实配置文件，也不会发出真实网络请求。
目标是只验证抓取脚本的核心行为：
1. 登录、网页解析和 API 结果拼装。
2. 输出文件名/目录的生成与 IMDb/TMDb 回填逻辑。
3. 主入口的并发调度、失败分支和短路行为。
"""

import copy
import importlib.util
import json
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

import requests

requests.packages.urllib3.disable_warnings()

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_yts.py"


def load_scrapy_yts(config: dict | None = None):
    """
    在隔离环境中加载 ``scrapy_yts`` 模块。

    被测模块在 import 时就会读取配置并导入 ``my_module`` / ``retrying`` /
    ``sort_movie_mysql`` / ``sort_movie_request``，所以这里先注入假的依赖，
    避免测试依赖本地真实环境。
    """
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "yts_url": "https://example.com",
        "yts_user": "user",
        "yts_pass": "pass",
        "thread_number": 2,
        "api_path": "https://example.com/api/v2/movie_details.json?movie_id=",
        "output_dir": temp_dir.name,
        "headers": {"User-Agent": "unit-test"},
        "cookie": "cookie=value",
    }
    if config:
        module_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: copy.deepcopy(module_config)
    fake_my_module.sanitize_filename = lambda name: name

    def fake_write_dict_to_json(path: str, data: dict) -> None:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    fake_my_module.write_dict_to_json = fake_write_dict_to_json
    fake_my_module.read_file_to_list = lambda _path: []

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    fake_sort_movie_mysql = types.ModuleType("sort_movie_mysql")
    fake_sort_movie_mysql.query_imdb_local_director = lambda _movie_id: []

    fake_sort_movie_request = types.ModuleType("sort_movie_request")
    fake_sort_movie_request.get_tmdb_movie_details = lambda _movie_id: None

    spec = importlib.util.spec_from_file_location(
        f"scrapy_yts_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "retrying": fake_retrying,
            "sort_movie_mysql": fake_sort_movie_mysql,
            "sort_movie_request": fake_sort_movie_request,
        },
    ):
        spec.loader.exec_module(module)

    return module, temp_dir


def build_movie_page(movie_id: str | None = "65882", director: str | None = "Director Name") -> str:
    """构造一个最小可用的电影详情页 HTML。"""
    movie_info = ""
    if movie_id is not None:
        movie_info = f'<div id="movie-info" data-movie-id="{movie_id}"></div>'

    director_html = ""
    if director is not None:
        director_html = (
            '<span itemprop="director">'
            f'<span itemprop="name">{director}</span>'
            '</span>'
        )

    return f"<html><body>{movie_info}{director_html}</body></html>"


def build_movie_detail(
        movie_id: int = 65882,
        title: str = "Movie",
        year: int = 2024,
        imdb_code: str = "tt1234567",
        torrents: list | None = None,
) -> dict:
    """构造一个最小可用的 API 返回结构。"""
    if torrents is None:
        torrents = [{"quality": "720p"}, {"quality": "1080p"}]

    return {
        "data": {
            "movie": {
                "id": movie_id,
                "title": title,
                "year": year,
                "imdb_code": imdb_code,
                "torrents": torrents,
            }
        }
    }


class TestYtsLogin(unittest.TestCase):
    """验证登录逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_scrapy_yts_injects_cookie_into_headers(self):
        """模块加载时应把配置里的 cookie 注入请求头。"""
        self.assertEqual(self.module.HEADERS["cookie"], "cookie=value")

    def test_yts_login_returns_true_when_status_is_ok(self):
        """登录接口返回 ``status=ok`` 时应判定为成功。"""
        session = Mock()
        response = Mock(text="Ok.")
        response.json.return_value = {"status": "ok"}
        session.post.return_value = response

        with patch("builtins.print"):
            result = self.module.yts_login(session)

        self.assertTrue(result)
        session.post.assert_called_once_with(
            "https://example.com/ajax/login",
            headers=self.module.HEADERS,
            data={"username": "user", "password": "pass"},
        )

    def test_yts_login_returns_false_when_status_is_not_ok(self):
        """登录接口返回非 ``ok`` 状态时应判定为失败。"""
        session = Mock()
        response = Mock(text="Denied")
        response.json.return_value = {"status": "error"}
        session.post.return_value = response

        with patch("builtins.print"):
            result = self.module.yts_login(session)

        self.assertFalse(result)


class TestFetchData(unittest.TestCase):
    """验证电影页和 API 数据的组合逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_fetch_data_returns_movie_detail_with_director(self):
        """网页存在电影 ID 和导演名时，应返回合并后的 JSON 数据。"""
        session = Mock()
        page_response = Mock(text=build_movie_page(movie_id="65882", director="Pat Director"))
        api_response = Mock()
        api_response.json.return_value = build_movie_detail(movie_id=65882)
        session.get.side_effect = [page_response, api_response]

        result = self.module.fetch_data(session, "https://example.com/movies/movie-a")

        self.assertEqual(result["data"]["movie"]["director"], "Pat Director")
        self.assertEqual(
            session.get.call_args_list,
            [
                (
                    ("https://example.com/movies/movie-a",),
                    {"headers": self.module.HEADERS, "verify": False},
                ),
                (
                    ("https://example.com/api/v2/movie_details.json?movie_id=65882",),
                    {"headers": self.module.HEADERS, "verify": False},
                ),
            ],
        )

    def test_fetch_data_uses_placeholder_when_director_is_missing(self):
        """网页中没有导演信息时，应回填默认占位符 ``_``。"""
        session = Mock()
        page_response = Mock(text=build_movie_page(movie_id="65882", director=None))
        api_response = Mock()
        api_response.json.return_value = build_movie_detail(movie_id=65882)
        session.get.side_effect = [page_response, api_response]

        result = self.module.fetch_data(session, "https://example.com/movies/movie-a")

        self.assertEqual(result["data"]["movie"]["director"], "_")

    def test_fetch_data_returns_empty_dict_when_movie_id_is_missing(self):
        """网页中找不到电影 ID 时，应直接返回空字典。"""
        session = Mock()
        page_response = Mock(text=build_movie_page(movie_id=None, director="Pat Director"))
        session.get.return_value = page_response

        result = self.module.fetch_data(session, "https://example.com/movies/movie-a")

        self.assertEqual(result, {})
        session.get.assert_called_once_with(
            "https://example.com/movies/movie-a",
            headers=self.module.HEADERS,
            verify=False,
        )

    def test_fetch_data_returns_empty_dict_when_api_has_no_valid_id(self):
        """API 返回结构存在但电影 ID 为空时，应返回空字典。"""
        session = Mock()
        page_response = Mock(text=build_movie_page(movie_id="65882", director="Pat Director"))
        api_response = Mock()
        api_response.json.return_value = build_movie_detail(movie_id=0)
        session.get.side_effect = [page_response, api_response]

        result = self.module.fetch_data(session, "https://example.com/movies/movie-a")

        self.assertEqual(result, {})


class TestGetBestQuality(unittest.TestCase):
    """验证最佳画质选择逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_best_quality_returns_highest_progressive_quality(self):
        """应选出 ``torrents`` 中数值最高的 ``*p`` 画质。"""
        result = {
            "data": {
                "movie": {
                    "torrents": [
                        {"quality": "720p"},
                        {"quality": "2160p"},
                        {"quality": "1080p"},
                    ]
                }
            }
        }

        self.assertEqual(self.module.get_best_quality(result), "2160p")

    def test_get_best_quality_returns_empty_string_when_no_valid_quality_exists(self):
        """没有合法 ``*p`` 画质时应返回空字符串。"""
        result = {
            "data": {
                "movie": {
                    "torrents": [
                        {"quality": "BluRay"},
                        {"quality": "4k"},
                        {"quality": ""},
                    ]
                }
            }
        }

        self.assertEqual(self.module.get_best_quality(result), "")


class TestHandleResult(unittest.TestCase):
    """验证结果落盘逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_handle_result_builds_output_path_and_writes_json(self):
        """落盘时应拼出导演目录和经过清洗的 JSON 文件名。"""
        result = build_movie_detail(
            title="Movie Name",
            year=2024,
            imdb_code="tt7654321",
            torrents=[{"quality": "720p"}, {"quality": "1080p"}],
        )
        result["data"]["movie"]["director"] = "Pat Director"

        with patch.object(self.module, "sanitize_filename", return_value="safe-name.json") as mock_sanitize, patch.object(
                self.module, "write_dict_to_json"
        ) as mock_write:
            self.module.handle_result(result, "https://example.com/movies/movie-a")

        mock_sanitize.assert_called_once_with("Movie Name(2024)[1080p]{tt7654321}.json")
        mock_write.assert_called_once_with(
            str(Path(self.temp_dir.name) / "Pat Director" / "safe-name.json"),
            result,
        )


class TestSearchImdbLocal(unittest.TestCase):
    """验证本地 IMDb 查询结果处理。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_search_imdb_local_returns_first_non_empty_director(self):
        """应返回去空白后的第一个有效导演名。"""
        with patch.object(
                self.module,
                "query_imdb_local_director",
                return_value=[{"director_name": "   "}, {"director_name": "  Jane Doe  "}],
        ):
            result = self.module.search_imdb_local("tt1234567")

        self.assertEqual(result, "Jane Doe")

    def test_search_imdb_local_returns_empty_string_when_query_fails(self):
        """本地 IMDb 查询返回 ``None`` 时应回空字符串。"""
        with patch.object(self.module, "query_imdb_local_director", return_value=None):
            result = self.module.search_imdb_local("tt1234567")

        self.assertEqual(result, "")


class TestScrapyYtsFixImdb(unittest.TestCase):
    """验证后处理目录整理逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()
        self.base_dir = Path(self.temp_dir.name)
        self.miss_dir = self.base_dir / "_"
        self.miss_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_yts_fix_imdb_moves_file_to_local_director_folder(self):
        """本地 IMDb 能查到导演时，应直接移动到对应目录。"""
        source_file = self.miss_dir / "Movie {tt1234567}.json"
        source_file.write_text("{}", encoding="utf-8")

        with patch.object(self.module, "search_imdb_local", return_value=' "Jane Doe" '), patch.object(
                self.module, "get_tmdb_movie_details"
        ) as mock_tmdb:
            self.module.scrapy_yts_fix_imdb(str(self.miss_dir))

        target_file = self.base_dir / "Jane Doe" / source_file.name
        self.assertFalse(source_file.exists())
        self.assertTrue(target_file.exists())
        mock_tmdb.assert_not_called()

    def test_scrapy_yts_fix_imdb_falls_back_to_tmdb_director(self):
        """本地 IMDb 查不到时，应回退到 TMDb 的导演信息。"""
        source_file = self.miss_dir / "Movie {tt7654321}.json"
        source_file.write_text("{}", encoding="utf-8")
        movie_details = {
            "casts": {
                "crew": [
                    {"job": "Writer", "name": "Someone Else"},
                    {"job": "Director", "name": "Pat Director"},
                ]
            }
        }

        with patch.object(self.module, "search_imdb_local", return_value=""), patch.object(
                self.module, "get_tmdb_movie_details", return_value=movie_details
        ):
            self.module.scrapy_yts_fix_imdb(str(self.miss_dir))

        self.assertTrue((self.base_dir / "Pat Director" / source_file.name).exists())

    def test_scrapy_yts_fix_imdb_uses_default_folder_when_no_director_is_found(self):
        """IMDb 和 TMDb 都查不到导演时，应移动到 ``没有导演`` 目录。"""
        source_file = self.miss_dir / "Movie {tt1111111}.json"
        source_file.write_text("{}", encoding="utf-8")

        with patch.object(self.module, "search_imdb_local", return_value=""), patch.object(
                self.module, "get_tmdb_movie_details", return_value=None
        ):
            self.module.scrapy_yts_fix_imdb(str(self.miss_dir))

        self.assertTrue((self.base_dir / "没有导演" / source_file.name).exists())

    def test_scrapy_yts_fix_imdb_skips_files_without_imdb_id(self):
        """文件名里没有 ``tt`` 编号时，不应尝试移动文件。"""
        source_file = self.miss_dir / "Movie without imdb.json"
        source_file.write_text("{}", encoding="utf-8")

        with patch.object(self.module, "search_imdb_local") as mock_search:
            self.module.scrapy_yts_fix_imdb(str(self.miss_dir))

        self.assertTrue(source_file.exists())
        mock_search.assert_not_called()


class TestScrapyYtsEntrypoint(unittest.TestCase):
    """验证主抓取入口的调度逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_yts_returns_early_when_login_fails(self):
        """登录失败时应直接返回，不再读取链接列表。"""
        fake_session = Mock()

        with patch.object(self.module.requests, "Session", return_value=fake_session), patch.object(
                self.module, "yts_login", return_value=False
        ) as mock_login, patch.object(self.module, "read_file_to_list") as mock_read_links, patch.object(
                self.module, "handle_result"
        ) as mock_handle, patch.object(self.module, "scrapy_yts_fix_imdb") as mock_fix:
            self.module.scrapy_yts("urls.txt")

        mock_login.assert_called_once_with(fake_session)
        mock_read_links.assert_not_called()
        mock_handle.assert_not_called()
        mock_fix.assert_not_called()

    def test_scrapy_yts_processes_success_and_skips_failed_links(self):
        """入口函数应处理成功结果，并吞掉空结果和子任务异常。"""
        fake_session = Mock()
        result_ok = build_movie_detail(title="Movie A", imdb_code="tt1000001")
        result_ok["data"]["movie"]["director"] = "Pat Director"

        def fake_fetch(_session, link: str):
            if link == "ok":
                return result_ok
            if link == "empty":
                return {}
            raise RuntimeError("boom")

        with patch.object(self.module.requests, "Session", return_value=fake_session), patch.object(
                self.module, "yts_login", return_value=True
        ), patch.object(
            self.module, "read_file_to_list", return_value=["ok", "empty", "boom"]
        ) as mock_read_links, patch.object(
            self.module, "fetch_data", side_effect=fake_fetch
        ), patch.object(
            self.module, "handle_result"
        ) as mock_handle, patch.object(self.module, "scrapy_yts_fix_imdb") as mock_fix:
            self.module.scrapy_yts("urls.txt")

        mock_read_links.assert_called_once_with("urls.txt")
        mock_handle.assert_called_once_with(result_ok, "ok")
        mock_fix.assert_not_called()
        self.assertEqual(fake_session.mount.call_count, 2)

    def test_scrapy_yts_calls_fix_imdb_when_missing_director_is_found(self):
        """本轮抓取结果里出现 ``_`` 导演时，应自动触发后处理补全。"""
        fake_session = Mock()
        result_missing_director = build_movie_detail(title="Movie A", imdb_code="tt1000002")
        result_missing_director["data"]["movie"]["director"] = "_"

        with patch.object(self.module.requests, "Session", return_value=fake_session), patch.object(
                self.module, "yts_login", return_value=True
        ), patch.object(
            self.module, "read_file_to_list", return_value=["missing"]
        ), patch.object(
            self.module, "fetch_data", return_value=result_missing_director
        ), patch.object(
            self.module, "handle_result"
        ) as mock_handle, patch.object(self.module, "scrapy_yts_fix_imdb") as mock_fix:
            self.module.scrapy_yts("urls.txt")

        mock_handle.assert_called_once_with(result_missing_director, "missing")
        mock_fix.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
