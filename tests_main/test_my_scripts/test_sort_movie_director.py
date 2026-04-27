"""
针对 ``my_scripts.sort_movie_director`` 的定向单元测试。

这些测试只覆盖当前整理导演流程里最重要的几段行为：
1. 主流程会收集 ``tt...``、写编号文件、创建 alias 文件并在拿到 TMDB 编号时移动目录。
2. 编号解析会先读现有空文件；缺失时再走 IMDb 本地库、TMDB、豆瓣补齐。
3. TMDB 电影回退匹配和豆瓣 alias 提取按当前脚本逻辑工作。
4. 导演作品列表收集会写 ``movies.csv``，并按当前规则筛选、整理电影信息。
"""

import csv
import importlib.util
import re
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "sort_movie_director.py"


def fake_scan_ids(path: str) -> dict[str, str | None]:
    """扫描目录中的 ``.imdb/.tmdb/.douban`` 空文件。"""
    result = {"imdb": None, "tmdb": None, "douban": None}
    for item in Path(path).iterdir():
        if not item.is_file():
            continue
        if item.suffix == ".imdb":
            result["imdb"] = item.stem
        elif item.suffix == ".tmdb":
            result["tmdb"] = item.stem
        elif item.suffix == ".douban":
            result["douban"] = item.stem
    return result


def fake_create_aka_director(path: str, aka: list[str]) -> None:
    """按去重后的 alias 列表创建空文件。"""
    for name in dict.fromkeys(aka):
        Path(path, name).touch()


def fake_extract_imdb_id(text: str) -> str | None:
    """从文本里提取第一个 ``tt...``。"""
    match = re.search(r"tt\d+", text)
    return match.group(0) if match else None


def load_sort_movie_director():
    """在隔离依赖的环境中加载 ``sort_movie_director`` 模块。"""
    fake_sort_movie_mysql = types.ModuleType("sort_movie_mysql")
    fake_sort_movie_mysql.query_imdb_title_directors = lambda _movie_id: []
    fake_sort_movie_mysql.insert_movie_wanted = lambda _wanted_list: None
    fake_sort_movie_mysql.remove_existing_tmdb_ids = lambda ids: ids

    fake_sort_movie_ops = types.ModuleType("sort_movie_ops")
    fake_sort_movie_ops.scan_ids = fake_scan_ids
    fake_sort_movie_ops.split_director_name = lambda text: [text]
    fake_sort_movie_ops.create_aka_director = fake_create_aka_director
    fake_sort_movie_ops.fix_douban_name = lambda text: text.strip()
    fake_sort_movie_ops.extract_imdb_id = fake_extract_imdb_id
    fake_sort_movie_ops.check_local_torrent = lambda _imdb, _quality, _source: {
        "move_counts": 0,
        "delete_counts": 0,
        "delete_files": [],
    }

    fake_sort_movie_request = types.ModuleType("sort_movie_request")
    fake_sort_movie_request.get_tmdb_director_details = lambda _director_id: {"name": "", "also_known_as": []}
    fake_sort_movie_request.get_tmdb_search_response = lambda _query: {}
    fake_sort_movie_request.get_tmdb_director_movies = lambda _director_id: {}
    fake_sort_movie_request.get_tmdb_movie_details = lambda _movie_id: {"casts": {"crew": []}}
    fake_sort_movie_request.get_douban_response = lambda _query, _mode: None
    fake_sort_movie_request.get_douban_search_details = lambda _response: None
    fake_sort_movie_request.check_kpk_for_better_quality = lambda _imdb, _quality: False
    fake_sort_movie_request.log_jackett_search_results = lambda _imdb: None

    spec = importlib.util.spec_from_file_location(
        f"sort_movie_director_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "sort_movie_mysql": fake_sort_movie_mysql,
            "sort_movie_ops": fake_sort_movie_ops,
            "sort_movie_request": fake_sort_movie_request,
        },
    ):
        spec.loader.exec_module(module)

    return module


class ImmediateFuture:
    """同步 future，避免单元测试依赖真实线程调度。"""

    def __init__(self, value):
        self._value = value

    def result(self):
        return self._value


class ImmediateExecutor:
    """同步执行器，实现被测代码所需的最小接口。"""

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def submit(self, func, *args, **kwargs):
        return ImmediateFuture(func(*args, **kwargs))


class TestSortDirectorAuto(unittest.TestCase):
    """验证主流程编排。"""

    def setUp(self):
        self.module = load_sort_movie_director()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.source_dir = Path(self.temp_dir.name) / "Director Name"
        self.source_dir.mkdir()
        (self.source_dir / "Movie Title [tt1234567].mkv").write_text("", encoding="utf-8")
        self.dst_root = Path(self.temp_dir.name) / "sorted"
        self.dst_root.mkdir()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_sort_director_auto_writes_ids_creates_aliases_and_moves_directory(self):
        """拿到 TMDB 编号时，应写入空文件、创建 alias 文件并移动目录。"""
        with patch.object(
            self.module,
            "get_director_ids",
            return_value=("nm0000001", "123", "456"),
        ) as mock_get_ids, patch.object(
            self.module,
            "get_director_aliases",
            return_value=["Director Name", "Alias A", "Alias B"],
        ) as mock_get_aliases:
            self.module.sort_director_auto(str(self.source_dir), str(self.dst_root))

        target_dir = self.dst_root / "Director Name"
        self.assertFalse(self.source_dir.exists())
        self.assertTrue(target_dir.exists())
        mock_get_ids.assert_called_once_with(
            str(self.source_dir),
            "Director Name",
            ["tt1234567"],
        )
        mock_get_aliases.assert_called_once_with("Director Name", "123", "456")

        self.assertTrue((target_dir / "nm0000001.imdb").exists())
        self.assertTrue((target_dir / "123.tmdb").exists())
        self.assertTrue((target_dir / "456.douban").exists())
        self.assertTrue((target_dir / "Director Name").exists())
        self.assertTrue((target_dir / "Alias A").exists())
        self.assertTrue((target_dir / "Alias B").exists())

    def test_sort_director_auto_returns_early_when_no_imdb_ids_found(self):
        """目录里没有 ``tt...`` 时，应直接返回且不继续查导演编号。"""
        empty_dir = Path(self.temp_dir.name) / "No IMDb Director"
        empty_dir.mkdir()
        (empty_dir / "notes.txt").write_text("no imdb here", encoding="utf-8")

        with patch.object(self.module, "get_director_ids") as mock_get_ids:
            self.module.sort_director_auto(str(empty_dir), str(self.dst_root))

        mock_get_ids.assert_not_called()
        self.assertTrue(empty_dir.exists())


class TestDirectorIds(unittest.TestCase):
    """验证导演编号解析流程。"""

    def setUp(self):
        self.module = load_sort_movie_director()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.source_dir = Path(self.temp_dir.name) / "Director Name"
        self.source_dir.mkdir()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_director_ids_uses_existing_marker_files(self):
        """目录里已有编号空文件时，不应再走补齐查询。"""
        (self.source_dir / "nm0000001.imdb").touch()
        (self.source_dir / "123.tmdb").touch()
        (self.source_dir / "456.douban").touch()

        with patch.object(self.module, "get_imdb_local_director") as mock_imdb, patch.object(
            self.module, "get_tmdb_director_id"
        ) as mock_tmdb, patch.object(self.module, "get_douban_director_id") as mock_douban:
            result = self.module.get_director_ids(str(self.source_dir), "Director Name", ["tt1234567"])

        self.assertEqual(result, ("nm0000001", "123", "456"))
        mock_imdb.assert_not_called()
        mock_tmdb.assert_not_called()
        mock_douban.assert_not_called()

    def test_get_director_ids_resolves_missing_ids_from_lookup_chain(self):
        """缺少空文件时，应按 IMDb 本地库、TMDB、豆瓣顺序补齐。"""
        with patch.object(
            self.module,
            "get_imdb_local_director",
            side_effect=[None, "nm0000002"],
        ) as mock_imdb, patch.object(
            self.module,
            "get_tmdb_director_id",
            return_value="789",
        ) as mock_tmdb, patch.object(
            self.module,
            "get_douban_director_id",
            return_value="654",
        ) as mock_douban:
            result = self.module.get_director_ids(
                str(self.source_dir),
                "Director Name",
                ["tt0000001", "tt0000002"],
            )

        self.assertEqual(result, ("nm0000002", "789", "654"))
        self.assertEqual(
            [call.args for call in mock_imdb.call_args_list],
            [("tt0000001", "Director Name"), ("tt0000002", "Director Name")],
        )
        mock_tmdb.assert_called_once_with("nm0000002", "Director Name", ["tt0000001", "tt0000002"])
        mock_douban.assert_called_once_with("nm0000002")


class TestTmdbAndDoubanHelpers(unittest.TestCase):
    """验证 TMDB 回退匹配和豆瓣 alias 提取。"""

    def setUp(self):
        self.module = load_sort_movie_director()

    def test_get_tmdb_director_id_falls_back_to_movie_crew_alias_matching(self):
        """TMDB 人物搜索为空时，应能通过电影主创 alias 反推导演。"""
        search_results = [
            {"person_results": []},
            {"movie_results": [{"id": 9001}]},
        ]
        movie_details = {
            "casts": {
                "crew": [
                    {"job": "Director", "id": 77, "name": "A. Tarkovsky"},
                    {"job": "Writer", "id": 88, "name": "Other Name"},
                ]
            }
        }

        with patch.object(
            self.module,
            "get_tmdb_search_response",
            side_effect=search_results,
        ) as mock_search, patch.object(
            self.module,
            "get_tmdb_movie_details",
            return_value=movie_details,
        ) as mock_movie, patch.object(
            self.module,
            "get_tmdb_director_aliases",
            return_value=("Andrei Tarkovsky", "Андрей Тарковский"),
        ) as mock_aliases:
            result = self.module.get_tmdb_director_id("nm0000001", "Андрей Тарковский", ["tt7654321"])

        self.assertEqual(result, "77")
        self.assertEqual(
            [call.args for call in mock_search.call_args_list],
            [("nm0000001",), ("tt7654321",)],
        )
        mock_movie.assert_called_once_with(9001)
        mock_aliases.assert_called_once_with("77")

    def test_get_douban_director_aliases_extracts_main_name_and_extra_aliases(self):
        """豆瓣人物页应提取主名字和“更多外文名”里的 alias。"""
        response = Mock(
            text="""
            <html>
              <h1 class="subject-name">黑泽明</h1>
              <span class="label">更多外文名:</span>
              <span class="value">Akira Kurosawa / Kurosawa Akira</span>
            </html>
            """
        )

        with patch.object(self.module, "get_douban_response", return_value=response):
            result = self.module.get_douban_director_aliases("123456")

        self.assertEqual(result, ["黑泽明", "Akira Kurosawa", "Kurosawa Akira"])


class TestDirectorMovieCollection(unittest.TestCase):
    """验证导演作品列表收集。"""

    def setUp(self):
        self.module = load_sort_movie_director()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.source_dir = Path(self.temp_dir.name) / "Director Name"
        self.source_dir.mkdir()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_director_movies_writes_csv_and_returns_imdb_ids(self):
        movies = [
            {
                "year": "1999",
                "imdb": "tt0111111",
                "tmdb": "101",
                "runtime": 120,
                "titles": ["Title A", "Alias A"],
            },
            {
                "year": "",
                "imdb": "",
                "tmdb": "102",
                "runtime": "",
                "titles": ["Title B"],
            },
        ]

        with patch.object(self.module, "get_tmdb_director_movies_all", return_value=movies) as mock_collect:
            imdb_ids = self.module.get_director_movies(str(self.source_dir))

        self.assertEqual(imdb_ids, ["tt0111111"])
        mock_collect.assert_called_once_with(str(self.source_dir))

        output_csv = self.source_dir / "movies.csv"
        self.assertTrue(output_csv.exists())
        with output_csv.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.reader(handle))

        self.assertEqual(rows[0], ["year", "imdb", "tmdb", "runtime", "titles"])
        self.assertEqual(
            rows[1],
            ["1999年", "tt0111111.imdb", "101.tmdb", "120分钟", "['Title A', 'Alias A']"],
        )
        self.assertEqual(
            rows[2],
            ["无年份", "无编号", "102.tmdb", "无时长", "['Title B']"],
        )

    def test_get_director_movies_skips_when_movies_csv_exists(self):
        output_csv = self.source_dir / "movies.csv"
        output_csv.write_text("existing", encoding="utf-8")

        with patch.object(self.module, "get_tmdb_director_movies_all") as mock_collect:
            result = self.module.get_director_movies(str(self.source_dir))

        self.assertIsNone(result)
        mock_collect.assert_not_called()

    def test_get_tmdb_director_movies_all_filters_existing_and_sorts(self):
        movie_infos = {
            "crew": [
                {"job": "Director", "id": 7},
                {"job": "Writer", "id": 99},
                {"job": "Director", "id": 5},
                {"job": "Director", "id": 7},
            ]
        }

        def fake_fetch(movie_id: str):
            mapping = {
                "7": {
                    "director": "",
                    "year": "2005",
                    "imdb": "tt0000007",
                    "tmdb": "7",
                    "runtime": 95,
                    "titles": ["Later Movie"],
                },
                "5": {
                    "director": "",
                    "year": "1998",
                    "imdb": "tt0000005",
                    "tmdb": "5",
                    "runtime": 100,
                    "titles": ["Earlier Movie"],
                },
            }
            return mapping[movie_id]

        with patch.object(self.module, "scan_ids", return_value={"tmdb": "42"}) as mock_scan, patch.object(
            self.module, "get_tmdb_director_movies", return_value=movie_infos
        ) as mock_credits, patch.object(
            self.module, "remove_existing_tmdb_ids", side_effect=lambda ids: ids
        ) as mock_remove, patch.object(
            self.module, "fetch_movie_info", side_effect=fake_fetch
        ) as mock_fetch, patch.object(
            self.module, "ThreadPoolExecutor", ImmediateExecutor
        ):
            movies = self.module.get_tmdb_director_movies_all(str(self.source_dir), skip_existing=True)

        self.assertEqual([item["tmdb"] for item in movies], ["5", "7"])
        self.assertEqual([item["year"] for item in movies], ["1998", "2005"])
        self.assertTrue(all(item["director"] == "Director Name" for item in movies))
        mock_scan.assert_called_once_with(str(self.source_dir))
        mock_credits.assert_called_once_with("42")
        mock_remove.assert_called_once_with({"5", "7"})
        self.assertCountEqual(
            [call.args[0] for call in mock_fetch.call_args_list],
            ["5", "7"],
        )

    def test_get_tmdb_director_movies_all_returns_none_without_tmdb_id(self):
        with patch.object(self.module, "scan_ids", return_value={"tmdb": None}) as mock_scan, patch.object(
            self.module, "get_tmdb_director_movies"
        ) as mock_credits:
            movies = self.module.get_tmdb_director_movies_all(str(self.source_dir))

        self.assertIsNone(movies)
        mock_scan.assert_called_once_with(str(self.source_dir))
        mock_credits.assert_not_called()

    def test_get_tmdb_director_movies_all_returns_none_without_director_credits(self):
        movie_infos = {
            "crew": [
                {"job": "Writer", "id": 9},
                {"job": "Producer", "id": 10},
            ]
        }

        with patch.object(self.module, "scan_ids", return_value={"tmdb": "42"}), patch.object(
            self.module, "get_tmdb_director_movies", return_value=movie_infos
        ), patch.object(self.module, "fetch_movie_info") as mock_fetch:
            movies = self.module.get_tmdb_director_movies_all(str(self.source_dir))

        self.assertIsNone(movies)
        mock_fetch.assert_not_called()

    def test_get_tmdb_director_movies_all_returns_none_when_all_existing_filtered(self):
        movie_infos = {
            "crew": [
                {"job": "Director", "id": 7},
                {"job": "Director", "id": 5},
            ]
        }

        with patch.object(self.module, "scan_ids", return_value={"tmdb": "42"}), patch.object(
            self.module, "get_tmdb_director_movies", return_value=movie_infos
        ), patch.object(
            self.module, "remove_existing_tmdb_ids", return_value=set()
        ) as mock_remove, patch.object(
            self.module, "fetch_movie_info"
        ) as mock_fetch:
            movies = self.module.get_tmdb_director_movies_all(str(self.source_dir), skip_existing=True)

        self.assertIsNone(movies)
        mock_remove.assert_called_once_with({"5", "7"})
        mock_fetch.assert_not_called()


class TestFetchMovieInfo(unittest.TestCase):
    """验证单片信息整理。"""

    def setUp(self):
        self.module = load_sort_movie_director()

    def test_fetch_movie_info_extracts_expected_fields(self):
        detail = {
            "imdb_id": "tt1234567",
            "release_date": "2001-09-09",
            "runtime": 111,
            "original_title": "Original Title",
            "translations": {
                "translations": [
                    {"data": {"title": "French Title"}},
                    {"data": {"title": "ALT TITLE"}},
                ]
            },
            "titles": [
                {"title": "Alt Title"},
                {"title": "Original Title"},
            ],
        }

        with patch.object(self.module, "get_tmdb_movie_details", return_value=detail) as mock_details:
            movie = self.module.fetch_movie_info("123")

        mock_details.assert_called_once_with("123")
        self.assertEqual(movie["year"], "2001")
        self.assertEqual(movie["imdb"], "tt1234567")
        self.assertEqual(movie["tmdb"], "123")
        self.assertEqual(movie["runtime"], 111)
        self.assertEqual(movie["director"], "")
        self.assertEqual(set(movie["titles"]), {"Original Title", "French Title", "Alt Title"})
        self.assertEqual(len(movie["titles"]), 3)

    def test_fetch_movie_info_returns_none_when_request_raises(self):
        with patch.object(self.module, "get_tmdb_movie_details", side_effect=RuntimeError("boom")):
            movie = self.module.fetch_movie_info("999")

        self.assertIsNone(movie)

    def test_fetch_movie_info_handles_missing_titles_and_translations(self):
        detail = {
            "imdb_id": "tt7654321",
            "release_date": "1995-01-01",
            "runtime": 88,
            "original_title": "Only Original",
        }

        with patch.object(self.module, "get_tmdb_movie_details", return_value=detail):
            movie = self.module.fetch_movie_info("456")

        self.assertEqual(movie["year"], "1995")
        self.assertEqual(movie["imdb"], "tt7654321")
        self.assertEqual(movie["titles"], ["Only Original"])


class TestAchieveDirector(unittest.TestCase):
    """验证导演归档补齐流程。"""

    def setUp(self):
        self.module = load_sort_movie_director()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.source_dir = Path(self.temp_dir.name) / "Director Name"
        self.source_dir.mkdir()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_collect_missing_director_movies_inserts_wanted_and_returns_movies(self):
        movies = [{"imdb": "tt1234567", "year": "1999", "titles": ["Title A"]}]

        with patch.object(
            self.module, "get_tmdb_director_movies_all", return_value=movies
        ) as mock_collect, patch.object(self.module, "insert_movie_wanted") as mock_insert:
            result = self.module.collect_missing_director_movies(str(self.source_dir))

        self.assertEqual(result, movies)
        mock_collect.assert_called_once_with(str(self.source_dir), skip_existing=True)
        mock_insert.assert_called_once_with(movies)

    def test_search_missing_director_movies_only_processes_movies_with_imdb(self):
        movies = [
            {"imdb": "tt1234567", "year": "1999", "titles": ["Title A"]},
            {"imdb": "", "year": "2000", "titles": ["Title B"]},
        ]

        with patch.object(self.module, "check_kpk_for_better_quality") as mock_kpk, patch.object(
            self.module, "log_jackett_search_results"
        ) as mock_jackett, patch.object(
            self.module,
            "check_local_torrent",
            return_value={"move_counts": 0, "delete_counts": 0, "delete_files": []},
        ) as mock_local, patch.object(self.module.time, "sleep"):
            self.module.search_missing_director_movies("Director Name", movies)

        mock_kpk.assert_called_once_with("tt1234567", "240p")
        mock_jackett.assert_called_once_with("tt1234567")
        mock_local.assert_called_once_with("tt1234567", "240p", "None")

    def test_achieve_director_orchestrates_collect_and_search(self):
        movies = [{"imdb": "tt1234567", "year": "1999", "titles": ["Title A"]}]

        with patch.object(
            self.module, "collect_missing_director_movies", return_value=movies
        ) as mock_collect, patch.object(
            self.module, "search_missing_director_movies"
        ) as mock_search, patch.object(self.module.time, "sleep"):
            self.module.achieve_director(str(self.source_dir))

        mock_collect.assert_called_once_with(str(self.source_dir))
        mock_search.assert_called_once_with("Director Name", movies)


if __name__ == "__main__":
    unittest.main()
