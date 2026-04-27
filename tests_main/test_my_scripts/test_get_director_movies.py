"""
针对 ``my_scripts.get_director_movies`` 的定向单元测试。

这些测试只覆盖本次准备重构的核心行为：
1. 主流程会写出 ``movies.csv`` 并返回 IMDb 列表。
2. 已存在输出文件时会短路跳过。
3. 导演作品收集会筛选导演职位、支持已存在过滤，并按年份排序。
4. 单片详情会被整理成脚本当前使用的字典结构。
5. 缺少常见字段时仍能平稳返回。
"""

import csv
import importlib.util
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "get_director_movies.py"


def load_get_director_movies():
    """在隔离依赖的环境中加载 ``get_director_movies`` 模块。"""
    fake_sort_movie_mysql = types.ModuleType("sort_movie_mysql")
    fake_sort_movie_mysql.remove_existing_tmdb_ids = lambda ids: ids

    fake_sort_movie_ops = types.ModuleType("sort_movie_ops")
    fake_sort_movie_ops.scan_ids = lambda _source: {"tmdb": None, "douban": None, "imdb": None}

    fake_sort_movie_request = types.ModuleType("sort_movie_request")
    fake_sort_movie_request.get_tmdb_director_movies = lambda _director_id: {}
    fake_sort_movie_request.get_tmdb_movie_details = lambda _movie_id: {}

    spec = importlib.util.spec_from_file_location(
        f"get_director_movies_test_{uuid.uuid4().hex}",
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


class TestGetDirectorMoviesMain(unittest.TestCase):
    def setUp(self):
        self.module = load_get_director_movies()
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

        with patch.object(self.module, "scan_ids", return_value={"tmdb": "42"}) as mock_scan, \
                patch.object(self.module, "get_tmdb_director_movies", return_value=movie_infos) as mock_credits, \
                patch.object(self.module, "remove_existing_tmdb_ids", side_effect=lambda ids: ids) as mock_remove, \
                patch.object(self.module, "fetch_movie_info", side_effect=fake_fetch) as mock_fetch, \
                patch.object(self.module, "ThreadPoolExecutor", ImmediateExecutor):
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
        with patch.object(self.module, "scan_ids", return_value={"tmdb": None}) as mock_scan, \
                patch.object(self.module, "get_tmdb_director_movies") as mock_credits:
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

        with patch.object(self.module, "scan_ids", return_value={"tmdb": "42"}), \
                patch.object(self.module, "get_tmdb_director_movies", return_value=movie_infos), \
                patch.object(self.module, "fetch_movie_info") as mock_fetch:
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

        with patch.object(self.module, "scan_ids", return_value={"tmdb": "42"}), \
                patch.object(self.module, "get_tmdb_director_movies", return_value=movie_infos), \
                patch.object(self.module, "remove_existing_tmdb_ids", return_value=set()) as mock_remove, \
                patch.object(self.module, "fetch_movie_info") as mock_fetch:
            movies = self.module.get_tmdb_director_movies_all(str(self.source_dir), skip_existing=True)

        self.assertIsNone(movies)
        mock_remove.assert_called_once_with({"5", "7"})
        mock_fetch.assert_not_called()


class TestFetchMovieInfo(unittest.TestCase):
    def setUp(self):
        self.module = load_get_director_movies()

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


if __name__ == "__main__":
    unittest.main()
