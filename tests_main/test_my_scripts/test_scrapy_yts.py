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
        response.status_code = 200
        response.json.return_value = {"status": "ok"}
        session.post.return_value = response

        result = self.module.yts_login(session)

        self.assertTrue(result)
        session.post.assert_called_once_with(
            "https://example.com/ajax/login",
            headers=self.module.HEADERS,
            data={"username": "user", "password": "pass"},
            timeout=self.module.REQUEST_TIMEOUT,
        )
        response.raise_for_status.assert_called_once_with()

    def test_yts_login_returns_false_when_status_is_not_ok(self):
        """登录接口返回非 ``ok`` 状态时应判定为失败。"""
        session = Mock()
        response = Mock(text="Denied")
        response.status_code = 200
        response.json.return_value = {"status": "error"}
        session.post.return_value = response

        result = self.module.yts_login(session)

        self.assertFalse(result)
        response.raise_for_status.assert_called_once_with()

    def test_yts_login_returns_false_when_request_exception_is_raised(self):
        """登录请求异常时应返回 ``False``，而不是把异常继续抛出。"""
        session = Mock()
        session.post.side_effect = requests.RequestException("network down")

        result = self.module.yts_login(session)

        self.assertFalse(result)

    def test_yts_login_returns_false_when_response_is_not_json(self):
        """登录接口返回非 JSON 响应时应返回 ``False``。"""
        session = Mock()
        response = Mock(text="<html>blocked</html>")
        response.status_code = 200
        response.json.side_effect = ValueError("not json")
        session.post.return_value = response

        result = self.module.yts_login(session)

        self.assertFalse(result)
        response.raise_for_status.assert_called_once_with()


class TestParseYtsMoviePage(unittest.TestCase):
    """验证 YTS 电影详情页解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_yts_movie_page_returns_movie_id_and_director(self):
        """页面存在电影 ID 和导演名时应正确返回。"""
        movie_id, director_name = self.module.parse_yts_movie_page(
            build_movie_page(movie_id="65882", director="Pat Director"),
            "https://example.com/movies/movie-a",
        )

        self.assertEqual(movie_id, "65882")
        self.assertEqual(director_name, "Pat Director")

    def test_parse_yts_movie_page_uses_placeholder_when_director_is_missing(self):
        """页面没有导演名时应返回默认占位符。"""
        movie_id, director_name = self.module.parse_yts_movie_page(
            build_movie_page(movie_id="65882", director=None),
            "https://example.com/movies/movie-a",
        )

        self.assertEqual(movie_id, "65882")
        self.assertEqual(director_name, "_")

    def test_parse_yts_movie_page_returns_none_when_movie_id_is_missing(self):
        """页面缺少电影 ID 时应返回 ``(None, \"\")``。"""
        movie_id, director_name = self.module.parse_yts_movie_page(
            build_movie_page(movie_id=None, director="Pat Director"),
            "https://example.com/movies/movie-a",
        )

        self.assertIsNone(movie_id)
        self.assertEqual(director_name, "")


class TestFetchMovieDetailById(unittest.TestCase):
    """验证根据电影 ID 获取 API 详情的逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_fetch_movie_detail_by_id_returns_movie_detail(self):
        """API 返回有效电影 ID 时应直接返回 JSON 数据。"""
        session = Mock()
        response = Mock()
        response.json.return_value = build_movie_detail(movie_id=65882)
        session.get.return_value = response

        result = self.module.fetch_movie_detail_by_id(session, "65882", "https://example.com/movies/movie-a")

        self.assertEqual(result["data"]["movie"]["id"], 65882)
        session.get.assert_called_once_with(
            "https://example.com/api/v2/movie_details.json?movie_id=65882",
            headers=self.module.HEADERS,
            verify=False,
            timeout=self.module.REQUEST_TIMEOUT,
        )

    def test_fetch_movie_detail_by_id_returns_empty_dict_when_api_has_no_valid_id(self):
        """API 返回结构存在但电影 ID 为空时，应返回空字典。"""
        session = Mock()
        response = Mock()
        response.json.return_value = build_movie_detail(movie_id=0)
        session.get.return_value = response

        result = self.module.fetch_movie_detail_by_id(session, "65882", "https://example.com/movies/movie-a")

        self.assertEqual(result, {})


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
                    {"headers": self.module.HEADERS, "verify": False, "timeout": self.module.REQUEST_TIMEOUT},
                ),
                (
                    ("https://example.com/api/v2/movie_details.json?movie_id=65882",),
                    {"headers": self.module.HEADERS, "verify": False, "timeout": self.module.REQUEST_TIMEOUT},
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
            timeout=self.module.REQUEST_TIMEOUT,
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

    def test_build_result_file_name_sanitizes_generated_name(self):
        """应按既定规则生成文件名，并交给 ``sanitize_filename`` 清洗。"""
        result = build_movie_detail(
            title="Movie Name",
            year=2024,
            imdb_code="tt7654321",
            torrents=[{"quality": "720p"}, {"quality": "1080p"}],
        )
        result["data"]["movie"]["director"] = "Pat Director"

        with patch.object(self.module, "sanitize_filename", return_value="safe-name.json") as mock_sanitize:
            file_name = self.module.build_result_file_name(result)

        self.assertEqual(file_name, "safe-name.json")
        mock_sanitize.assert_called_once_with("Movie Name(2024)[1080p]{tt7654321}.json")

    def test_build_result_output_path_uses_normalized_director_folder(self):
        """输出路径应使用清洗后的导演目录名。"""
        result = build_movie_detail(
            title="Movie Name",
            year=2024,
            imdb_code="tt7654321",
            torrents=[{"quality": "1080p"}],
        )
        result["data"]["movie"]["director"] = ' "Pat Director" '

        with patch.object(self.module, "normalize_director_folder_name", return_value="Pat Director") as mock_normalize, patch.object(
                self.module, "build_result_file_name", return_value="movie.json"
        ):
            file_path = self.module.build_result_output_path(result)

        self.assertEqual(file_path, str(Path(self.temp_dir.name) / "Pat Director" / "movie.json"))
        mock_normalize.assert_called_once_with(' "Pat Director" ')

    def test_handle_result_builds_output_path_and_writes_json(self):
        """落盘时应拼出导演目录和经过清洗的 JSON 文件名。"""
        result = build_movie_detail(
            title="Movie Name",
            year=2024,
            imdb_code="tt7654321",
            torrents=[{"quality": "720p"}, {"quality": "1080p"}],
        )
        result["data"]["movie"]["director"] = ' "Pat Director" '

        with patch.object(self.module, "sanitize_filename", return_value="safe-name.json") as mock_sanitize, patch.object(
                self.module, "normalize_director_folder_name", return_value="Pat Director"
        ) as mock_normalize, patch.object(self.module, "write_dict_to_json"
        ) as mock_write:
            self.module.handle_result(result, "https://example.com/movies/movie-a")

        mock_sanitize.assert_called_once_with("Movie Name(2024)[1080p]{tt7654321}.json")
        mock_normalize.assert_called_once_with(' "Pat Director" ')
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


class TestExtractImdbIdFromFilename(unittest.TestCase):
    """验证从文件名提取 IMDb 编号的逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_extract_imdb_id_from_filename_returns_matched_id(self):
        """文件名中存在 ``tt`` 编号时应正确提取。"""
        result = self.module.extract_imdb_id_from_filename("Movie Name {tt1234567}.json")

        self.assertEqual(result, "tt1234567")

    def test_extract_imdb_id_from_filename_returns_none_when_missing(self):
        """文件名中没有 ``tt`` 编号时应返回 ``None``。"""
        result = self.module.extract_imdb_id_from_filename("Movie Name without imdb.json")

        self.assertIsNone(result)


class TestSearchTmdbDirector(unittest.TestCase):
    """验证 TMDb 导演查询结果处理。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_search_tmdb_director_returns_director_name(self):
        """TMDb 返回 crew 列表时应取第一个导演名。"""
        movie_details = {
            "casts": {
                "crew": [
                    {"job": "Writer", "name": "Someone Else"},
                    {"job": "Director", "name": "Pat Director"},
                ]
            }
        }

        with patch.object(self.module, "get_tmdb_movie_details", return_value=movie_details):
            result = self.module.search_tmdb_director("tt7654321")

        self.assertEqual(result, "Pat Director")

    def test_search_tmdb_director_returns_empty_string_when_no_director_exists(self):
        """TMDb 没有导演信息时应返回空字符串。"""
        movie_details = {
            "casts": {
                "crew": [
                    {"job": "Writer", "name": "Someone Else"},
                ]
            }
        }

        with patch.object(self.module, "get_tmdb_movie_details", return_value=movie_details):
            result = self.module.search_tmdb_director("tt7654321")

        self.assertEqual(result, "")


class TestNormalizeDirectorFolderName(unittest.TestCase):
    """验证导演目录名清洗逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_normalize_director_folder_name_strips_spaces_and_quotes(self):
        """应去掉首尾空白和双引号。"""
        result = self.module.normalize_director_folder_name('  "Jane Doe"  ')

        self.assertEqual(result, "Jane Doe")

    def test_normalize_director_folder_name_keeps_normal_name(self):
        """普通目录名应保持不变。"""
        result = self.module.normalize_director_folder_name("Pat Director")

        self.assertEqual(result, "Pat Director")


class TestResolveDirectorName(unittest.TestCase):
    """验证导演名决议顺序。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_resolve_director_name_prefers_local_imdb_result(self):
        """本地 IMDb 查到导演时应直接返回，不再查 TMDb。"""
        with patch.object(self.module, "search_imdb_local", return_value="Jane Doe"), patch.object(
                self.module, "search_tmdb_director"
        ) as mock_tmdb:
            result = self.module.resolve_director_name("tt1234567")

        self.assertEqual(result, "Jane Doe")
        mock_tmdb.assert_not_called()

    def test_resolve_director_name_falls_back_to_tmdb(self):
        """本地 IMDb 为空时应回退到 TMDb。"""
        with patch.object(self.module, "search_imdb_local", return_value=""), patch.object(
                self.module, "search_tmdb_director", return_value="Pat Director"
        ):
            result = self.module.resolve_director_name("tt7654321")

        self.assertEqual(result, "Pat Director")

    def test_resolve_director_name_uses_default_when_all_lookups_fail(self):
        """IMDb 和 TMDb 都失败时应返回默认目录名。"""
        with patch.object(self.module, "search_imdb_local", return_value=""), patch.object(
                self.module, "search_tmdb_director", return_value=""
        ):
            result = self.module.resolve_director_name("tt1111111")

        self.assertEqual(result, self.module.NO_DIRECTOR_NAME)


class TestMoveFileToDirectorFolder(unittest.TestCase):
    """验证文件移动到导演目录的逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()
        self.base_dir = Path(self.temp_dir.name)
        self.miss_dir = self.base_dir / "_"
        self.miss_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_move_file_to_director_folder_moves_file_and_creates_directory(self):
        """应创建导演目录并把源文件移动过去。"""
        source_file = self.miss_dir / "Movie {tt1234567}.json"
        source_file.write_text("{}", encoding="utf-8")

        target_file = self.module.move_file_to_director_folder(source_file, self.base_dir, "Jane Doe")

        self.assertFalse(source_file.exists())
        self.assertTrue((self.base_dir / "Jane Doe").exists())
        self.assertTrue(target_file.exists())

    def test_move_file_to_director_folder_returns_target_path(self):
        """返回值应为移动后的目标文件路径。"""
        source_file = self.miss_dir / "Movie {tt7654321}.json"
        source_file.write_text("{}", encoding="utf-8")

        target_file = self.module.move_file_to_director_folder(source_file, self.base_dir, "Pat Director")

        self.assertEqual(target_file, self.base_dir / "Pat Director" / source_file.name)


class TestProcessMissingDirectorFile(unittest.TestCase):
    """验证单个缺导演文件的处理逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_yts()
        self.base_dir = Path(self.temp_dir.name)
        self.miss_dir = self.base_dir / "_"
        self.miss_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_process_missing_director_file_moves_file_to_resolved_director_folder(self):
        """应解析 IMDb 编号、决议导演目录并完成移动。"""
        source_file = self.miss_dir / 'Movie {tt1234567}.json'
        source_file.write_text("{}", encoding="utf-8")

        with patch.object(self.module, "resolve_director_name", return_value=' "Jane Doe" '):
            self.module.process_missing_director_file(source_file, self.base_dir)

        self.assertFalse(source_file.exists())
        self.assertTrue((self.base_dir / "Jane Doe" / source_file.name).exists())

    def test_process_missing_director_file_returns_early_when_imdb_id_is_missing(self):
        """文件名中没有 IMDb 编号时应直接返回，不继续解析导演。"""
        source_file = self.miss_dir / "Movie without imdb.json"
        source_file.write_text("{}", encoding="utf-8")

        with patch.object(self.module, "resolve_director_name") as mock_resolve, patch.object(
                self.module, "move_file_to_director_folder"
        ) as mock_move:
            self.module.process_missing_director_file(source_file, self.base_dir)

        self.assertTrue(source_file.exists())
        mock_resolve.assert_not_called()
        mock_move.assert_not_called()


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

        with patch.object(self.module, "resolve_director_name", return_value=' "Jane Doe" '):
            self.module.scrapy_yts_fix_imdb(str(self.miss_dir))

        target_file = self.base_dir / "Jane Doe" / source_file.name
        self.assertFalse(source_file.exists())
        self.assertTrue(target_file.exists())

    def test_scrapy_yts_fix_imdb_delegates_each_json_file_to_helper(self):
        """主函数应只遍历目录，并把每个 JSON 文件交给单文件 helper。"""
        json_file = self.miss_dir / "Movie {tt1234567}.json"
        txt_file = self.miss_dir / "notes.txt"
        json_file.write_text("{}", encoding="utf-8")
        txt_file.write_text("skip", encoding="utf-8")

        with patch.object(self.module, "process_missing_director_file") as mock_process:
            self.module.scrapy_yts_fix_imdb(str(self.miss_dir))

        mock_process.assert_called_once_with(json_file, self.base_dir)

    def test_scrapy_yts_fix_imdb_falls_back_to_tmdb_director(self):
        """本地 IMDb 查不到时，应回退到 TMDb 的导演信息。"""
        source_file = self.miss_dir / "Movie {tt7654321}.json"
        source_file.write_text("{}", encoding="utf-8")

        with patch.object(self.module, "resolve_director_name", return_value="Pat Director"):
            self.module.scrapy_yts_fix_imdb(str(self.miss_dir))

        self.assertTrue((self.base_dir / "Pat Director" / source_file.name).exists())

    def test_scrapy_yts_fix_imdb_uses_default_folder_when_no_director_is_found(self):
        """IMDb 和 TMDb 都查不到导演时，应移动到 ``没有导演`` 目录。"""
        source_file = self.miss_dir / "Movie {tt1111111}.json"
        source_file.write_text("{}", encoding="utf-8")

        with patch.object(self.module, "resolve_director_name", return_value=self.module.NO_DIRECTOR_NAME):
            self.module.scrapy_yts_fix_imdb(str(self.miss_dir))

        self.assertTrue((self.base_dir / "没有导演" / source_file.name).exists())

    def test_scrapy_yts_fix_imdb_skips_files_without_imdb_id(self):
        """文件名里没有 ``tt`` 编号时，不应尝试移动文件。"""
        source_file = self.miss_dir / "Movie without imdb.json"
        source_file.write_text("{}", encoding="utf-8")

        with patch.object(self.module, "resolve_director_name") as mock_resolve, patch.object(
                self.module, "move_file_to_director_folder"
        ) as mock_move:
            self.module.scrapy_yts_fix_imdb(str(self.miss_dir))

        self.assertTrue(source_file.exists())
        mock_resolve.assert_not_called()
        mock_move.assert_not_called()


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

    def test_scrapy_yts_counts_exception_failures_in_summary(self):
        """异常失败和空结果失败都应计入最终失败数量。"""
        fake_session = Mock()

        def fake_fetch(_session, link: str):
            if link == "empty":
                return {}
            raise RuntimeError("boom")

        with patch.object(self.module.requests, "Session", return_value=fake_session), patch.object(
                self.module, "yts_login", return_value=True
        ), patch.object(
            self.module, "read_file_to_list", return_value=["empty", "boom"]
        ), patch.object(
            self.module, "fetch_data", side_effect=fake_fetch
        ), patch.object(
            self.module.logger, "warning"
        ) as mock_warning, patch.object(
            self.module.logger, "error"
        ) as mock_error, patch.object(
            self.module.logger, "exception"
        ), patch.object(
            self.module, "scrapy_yts_fix_imdb"
        ) as mock_fix:
            self.module.scrapy_yts("urls.txt")

        mock_warning.assert_called_once_with("总计数量：2，失败数量：2。失败链接：")
        self.assertEqual([call.args[0] for call in mock_error.call_args_list], ["empty", "boom"])
        mock_fix.assert_not_called()

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

    def test_scrapy_yts_logs_summary_even_when_fix_imdb_fails(self):
        """自动补导演失败时，不应影响抓取汇总日志输出。"""
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
        ), patch.object(
            self.module, "scrapy_yts_fix_imdb", side_effect=RuntimeError("fix failed")
        ), patch.object(
            self.module.logger, "warning"
        ) as mock_warning, patch.object(
            self.module.logger, "exception"
        ) as mock_exception:
            self.module.scrapy_yts("urls.txt")

        mock_warning.assert_called_once_with("总计数量：1，失败数量：0。失败链接：")
        self.assertIn("yts: 自动补导演时发生错误", [call.args[0] for call in mock_exception.call_args_list])

    def test_scrapy_yts_logs_stable_message_when_thread_setup_fails(self):
        """线程分配阶段异常时，外层日志不应依赖未定义的 ``link``。"""
        fake_session = Mock()

        class BrokenExecutor:
            def __init__(self, *args, **kwargs):
                raise RuntimeError("executor boom")

        with patch.object(self.module.requests, "Session", return_value=fake_session), patch.object(
                self.module, "yts_login", return_value=True
        ), patch.object(
            self.module, "read_file_to_list", return_value=["a", "b"]
        ), patch.object(
            self.module, "ThreadPoolExecutor", BrokenExecutor
        ), patch.object(
            self.module.logger, "exception"
        ) as mock_exception, patch.object(
            self.module.logger, "warning"
        ):
            self.module.scrapy_yts("urls.txt")

        mock_exception.assert_called_once_with("来源文件：urls.txt 在线程分配阶段发生错误")


if __name__ == "__main__":
    unittest.main()
