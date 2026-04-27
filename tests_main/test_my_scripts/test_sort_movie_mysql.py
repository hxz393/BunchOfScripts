"""
针对 ``my_scripts.sort_movie_mysql`` 的定向单元测试。

这些测试不依赖真实配置文件或真实 MySQL，重点覆盖当前脚本最关键的行为：
1. 配置加载后的连接创建参数。
2. 批量查重与 ``wanted`` 插入逻辑。
3. ``movies`` 插入与 ``wanted`` 清理处于同一事务。
4. 各类查询辅助函数的 SQL 调度和关闭行为。
5. 关键辅助函数的回退和异常路径。
"""

import importlib.util
import sys
import tempfile
import types
import unittest
import uuid
from collections import deque
from pathlib import Path
from unittest.mock import Mock, patch

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "sort_movie_mysql.py"


class FakeMySQLError(Exception):
    """测试用的 MySQL 异常类型。"""


class CursorStub:
    """实现被测代码需要的最小游标接口。"""

    def __init__(
        self,
        fetchone_results=None,
        fetchall_results=None,
        lastrowid=0,
        rowcount=0,
        execute_hook=None,
        executemany_hook=None,
    ):
        self.fetchone_results = deque(fetchone_results or [])
        self.fetchall_results = deque(fetchall_results or [])
        self.lastrowid = lastrowid
        self.rowcount = rowcount
        self.execute_hook = execute_hook
        self.executemany_hook = executemany_hook
        self.execute_calls = []
        self.executemany_calls = []
        self.closed = False

    def execute(self, sql, params=None):
        self.execute_calls.append((sql, params))
        if self.execute_hook is not None:
            self.execute_hook(sql, params)

    def executemany(self, sql, params_seq):
        params_list = list(params_seq)
        self.executemany_calls.append((sql, params_list))
        self.rowcount = len(params_list)
        if self.executemany_hook is not None:
            self.executemany_hook(sql, params_list)

    def fetchone(self):
        if self.fetchone_results:
            return self.fetchone_results.popleft()
        return None

    def fetchall(self):
        if self.fetchall_results:
            return self.fetchall_results.popleft()
        return []

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class ConnectionStub:
    """实现被测代码需要的最小连接接口。"""

    def __init__(self, cursors=None):
        self.cursors = list(cursors or [CursorStub()])
        self.cursor_calls = []
        self.commit_calls = 0
        self.rollback_calls = 0
        self.closed = False
        self.connected = True
        self._cursor_index = 0

    def cursor(self, dictionary=False):
        self.cursor_calls.append({"dictionary": dictionary})
        cursor = self.cursors[min(self._cursor_index, len(self.cursors) - 1)]
        self._cursor_index += 1
        return cursor

    def commit(self):
        self.commit_calls += 1

    def rollback(self):
        self.rollback_calls += 1

    def close(self):
        self.closed = True
        self.connected = False

    def is_connected(self):
        return self.connected


def build_movie_info(**overrides):
    """构造 ``sort_movie_mysql`` 所需的最小完整电影信息。"""
    data = {
        "director": "Director Name",
        "year": 2024,
        "original_title": "Original Title",
        "chinese_title": "中文标题",
        "genres": ["Drama"],
        "country": ["Japan"],
        "language": ["Japanese"],
        "runtime": 120,
        "titles": ["Original Title", "Alias Title"],
        "directors": ["Director Name"],
        "tmdb": "12345",
        "douban": "67890",
        "imdb": "tt1234567",
        "source": "blu-ray",
        "quality": "1080p",
        "resolution": "1920x1080",
        "codec": "x265",
        "bitrate": "10 Mbps",
        "duration": 7200,
        "size": 1234567890,
        "release_group": "GROUP",
        "filename": "movie.mkv",
        "version": "v1",
        "publisher": "publisher",
        "pubdate": "2024-01-01",
        "dvhdr": "DV",
        "audio": "DTS",
        "subtitle": "Chinese",
        "dl_link": "magnet:?xt=urn:btih:HASH",
        "comment": "comment",
    }
    data.update(overrides)
    return data


def load_sort_movie_mysql(config=None):
    """在隔离依赖的环境中加载 ``sort_movie_mysql`` 模块。"""
    module_config = {
        "mysql_host": "127.0.0.1",
        "mysql_user": "movie_user",
        "mysql_pass": "movie_pass",
        "mysql_db_movie": "movie_db",
        "mysql_db_imdb": "imdb_db",
    }
    if config:
        module_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: dict(module_config)

    fake_sort_movie_ops = types.ModuleType("sort_movie_ops")

    def fake_parse_movie_id(movie_id: str):
        if movie_id.startswith("tt"):
            return "imdb", movie_id
        if movie_id.startswith("tmdb"):
            return "tmdb", movie_id[4:]
        if movie_id.startswith("db"):
            return "douban", movie_id[2:]
        return None

    fake_sort_movie_ops.parse_movie_id = fake_parse_movie_id

    fake_mysql_connector = types.ModuleType("mysql.connector")
    fake_mysql_connector.Error = FakeMySQLError
    fake_mysql_connector.connect = Mock(name="connect")

    fake_mysql_pkg = types.ModuleType("mysql")
    fake_mysql_pkg.__path__ = []
    fake_mysql_pkg.connector = fake_mysql_connector

    spec = importlib.util.spec_from_file_location(
        f"sort_movie_mysql_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "sort_movie_ops": fake_sort_movie_ops,
            "mysql": fake_mysql_pkg,
            "mysql.connector": fake_mysql_connector,
        },
    ):
        spec.loader.exec_module(module)

    return module


class TestConnectionsAndBatchHelpers(unittest.TestCase):
    def setUp(self):
        self.module = load_sort_movie_mysql()

    def test_movies_insert_sql_placeholder_count_matches_columns(self):
        self.assertEqual(
            self.module.MOVIES_INSERT_SQL.count("%s"),
            len(self.module.MOVIES_INSERT_COLUMNS),
        )

    def test_build_movie_insert_data_follows_field_list_and_serializes_json_fields(self):
        movie_info = build_movie_info(titles=["原始标题", "Alias Title"])
        current_time = "2026-04-27 18:00:00"

        result = self.module.build_movie_insert_data(movie_info, current_time)

        self.assertEqual(len(result), len(self.module.MOVIES_INSERT_COLUMNS))
        self.assertEqual(result[0], "Director Name")
        self.assertEqual(
            result[self.module.MOVIE_INSERT_FIELDS.index("genres")],
            '["Drama"]',
        )
        self.assertEqual(
            result[self.module.MOVIE_INSERT_FIELDS.index("titles")],
            '["原始标题", "Alias Title"]',
        )
        self.assertEqual(result[-2:], (current_time, current_time))

    def test_build_movie_insert_data_raises_clear_key_error_for_missing_fields(self):
        movie_info = build_movie_info()
        del movie_info["director"]
        del movie_info["quality"]

        with self.assertRaises(KeyError) as cm:
            self.module.build_movie_insert_data(movie_info, "2026-04-27 18:00:00")

        self.assertEqual(
            cm.exception.args[0],
            "movie_info.json5 缺少必要字段: director, quality",
        )

    def test_create_conn_uses_main_database_config(self):
        sentinel = object()
        self.module.mysql.connector.connect.return_value = sentinel

        result = self.module.create_conn()

        self.assertIs(result, sentinel)
        self.module.mysql.connector.connect.assert_called_once_with(
            host="127.0.0.1",
            user="movie_user",
            password="movie_pass",
            database="movie_db",
        )

    def test_create_conn_uses_explicit_imdb_database_config(self):
        sentinel = object()
        self.module.mysql.connector.connect.return_value = sentinel

        result = self.module.create_conn(self.module.MYSQL_DB_IMDB)

        self.assertIs(result, sentinel)
        self.module.mysql.connector.connect.assert_called_once_with(
            host="127.0.0.1",
            user="movie_user",
            password="movie_pass",
            database="imdb_db",
        )

    def test_fetch_existing_values_returns_empty_set_without_query_for_empty_input(self):
        cursor = Mock()

        result = self.module.fetch_existing_values(cursor, "movies", "tmdb", set())

        self.assertEqual(result, set())
        cursor.execute.assert_not_called()

    def test_fetch_existing_values_queries_matching_values(self):
        cursor = Mock()
        cursor.fetchall.return_value = [("101",), ("202",)]

        result = self.module.fetch_existing_values(cursor, "movies", "tmdb", {"101", "202"})

        self.assertEqual(result, {"101", "202"})
        sql, params = cursor.execute.call_args.args
        self.assertIn("SELECT tmdb FROM movies WHERE tmdb IN", sql)
        self.assertEqual(set(params), {"101", "202"})

    def test_fetch_existing_tmdb_in_movies_and_wanted_merges_both_tables(self):
        cursor = CursorStub(fetchall_results=[[("101",)], [("202",)]])

        result = self.module.fetch_existing_tmdb_in_movies_and_wanted(cursor, {"101", "202", "303"})

        self.assertEqual(result, {"101", "202"})
        self.assertEqual(len(cursor.execute_calls), 2)
        self.assertIn("SELECT tmdb FROM movies WHERE tmdb IN", cursor.execute_calls[0][0])
        self.assertIn("SELECT tmdb FROM wanted WHERE tmdb IN", cursor.execute_calls[1][0])

    def test_get_batch_by_imdb_uses_dictionary_cursor(self):
        cursor = CursorStub(fetchall_results=[[{"imdb": "tt1"}]])
        conn = ConnectionStub([cursor])

        result = self.module.get_batch_by_imdb(conn, "wanted", {"tt1"})

        self.assertEqual(result, [{"imdb": "tt1"}])
        self.assertEqual(conn.cursor_calls, [{"dictionary": True}])
        self.assertTrue(cursor.closed)
        self.assertIn("SELECT * FROM wanted WHERE imdb IN", cursor.execute_calls[0][0])

    def test_get_batch_by_imdb_returns_empty_list_for_empty_ids(self):
        conn = ConnectionStub()

        result = self.module.get_batch_by_imdb(conn, "movies", set())

        self.assertEqual(result, [])
        self.assertEqual(conn.cursor_calls, [])

    def test_get_batch_by_imdb_rejects_unknown_table_name(self):
        conn = ConnectionStub()

        with self.assertRaises(ValueError):
            self.module.get_batch_by_imdb(conn, "invalid_table", {"tt1"})


class TestInsertMovieWanted(unittest.TestCase):
    def setUp(self):
        self.module = load_sort_movie_mysql()

    def test_insert_movie_wanted_skips_empty_input(self):
        with patch.object(self.module, "create_conn") as mock_create_conn:
            self.module.insert_movie_wanted([])

        mock_create_conn.assert_not_called()

    def test_insert_movie_wanted_deduplicates_existing_and_pending_tmdb(self):
        cursor = CursorStub(fetchall_results=[[("101",)], [("202",)]])
        conn = ConnectionStub([cursor])
        wanted_list = [
            {"director": "A", "year": 2001, "imdb": "tt0000001", "tmdb": "101", "runtime": 101, "titles": ["A"]},
            {"director": "B", "year": 2002, "imdb": "tt0000002", "tmdb": "202", "runtime": 102, "titles": ["B"]},
            {"director": "C", "year": 2003, "imdb": "tt0000003", "tmdb": "303", "runtime": 103, "titles": ["C"]},
            {"director": "D", "year": 2004, "imdb": "tt0000004", "tmdb": "303", "runtime": 104, "titles": ["D"]},
            {"director": "E", "year": "", "imdb": "", "tmdb": None, "runtime": 105, "titles": ["片名"]},
        ]

        with patch.object(self.module, "create_conn", return_value=conn):
            self.module.insert_movie_wanted(wanted_list)

        self.assertEqual(conn.commit_calls, 1)
        self.assertTrue(cursor.closed)
        self.assertTrue(conn.closed)
        self.assertEqual(len(cursor.executemany_calls), 1)
        sql, rows = cursor.executemany_calls[0]
        self.assertEqual(sql, self.module.WANTED_INSERT_SQL)
        self.assertEqual(
            rows,
            [
                ("C", 2003, "tt0000003", "303", 103, '["C"]'),
                ("E", 0, None, None, 105, '["片名"]'),
            ],
        )

    def test_insert_movie_wanted_rolls_back_and_reraises_on_mysql_error(self):
        def raise_mysql_error(_sql, _params_list):
            raise self.module.mysql.connector.Error("insert failed")

        cursor = CursorStub(executemany_hook=raise_mysql_error)
        conn = ConnectionStub([cursor])

        with patch.object(self.module, "create_conn", return_value=conn):
            with self.assertRaises(self.module.mysql.connector.Error):
                self.module.insert_movie_wanted(
                    [{"director": "A", "year": 2001, "imdb": "tt1", "tmdb": "303", "runtime": 101, "titles": ["A"]}]
                )

        self.assertEqual(conn.rollback_calls, 1)
        self.assertEqual(conn.commit_calls, 0)
        self.assertTrue(conn.closed)


class TestSortMovieMysqlMain(unittest.TestCase):
    def setUp(self):
        self.module = load_sort_movie_mysql()

    def test_sort_movie_mysql_returns_early_when_movie_info_missing(self):
        with patch.object(self.module, "read_json_to_dict", return_value=None), patch.object(
            self.module, "create_conn"
        ) as mock_create_conn, patch.object(self.module.logger, "error") as mock_error:
            self.module.insert_movie_record_to_mysql("D:\\movies\\Example")

        mock_create_conn.assert_not_called()
        mock_error.assert_called_with("无法读取 JSON 文件")

    def test_sort_movie_mysql_skips_existing_record_and_closes_resources(self):
        cursor = CursorStub()
        conn = ConnectionStub([cursor])

        with patch.object(self.module, "read_json_to_dict", return_value=build_movie_info()), patch.object(
            self.module, "create_conn", return_value=conn
        ), patch.object(self.module, "get_record_id_by_priority", return_value=55), patch.object(
            self.module.logger, "error"
        ) as mock_error:
            self.module.insert_movie_record_to_mysql("D:\\movies\\Existing")

        self.assertEqual(conn.commit_calls, 0)
        self.assertEqual(cursor.execute_calls, [])
        self.assertTrue(cursor.closed)
        self.assertTrue(conn.closed)
        mock_error.assert_called_with("已有记录，不执行插入。IMDB: tt1234567 ID: 55")

    def test_sort_movie_mysql_inserts_movie_and_deletes_wanted_in_same_transaction(self):
        cursor = CursorStub(lastrowid=99)
        conn = ConnectionStub([cursor])
        movie_info = build_movie_info(tmdb="44444", imdb="tt7654321")

        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            self.module, "read_json_to_dict", return_value=movie_info
        ) as mock_read_json, patch.object(
            self.module, "create_conn", return_value=conn
        ), patch.object(
            self.module, "get_record_id_by_priority", return_value=None
        ), patch.object(
            self.module.logger, "info"
        ) as mock_info:
            self.module.insert_movie_record_to_mysql(temp_dir)

        expected_json_path = str(Path(temp_dir) / "movie_info.json5")
        mock_read_json.assert_called_once_with(expected_json_path)
        self.assertEqual(conn.commit_calls, 1)
        self.assertEqual(conn.rollback_calls, 0)
        self.assertEqual(len(cursor.execute_calls), 2)
        self.assertIn("INSERT INTO movies", cursor.execute_calls[0][0])
        self.assertEqual(cursor.execute_calls[1], ("DELETE FROM wanted WHERE tmdb = %s", ("44444",)))
        self.assertTrue(cursor.closed)
        self.assertTrue(conn.closed)
        mock_info.assert_any_call("已插入数据库！IMDB: tt7654321")

    def test_sort_movie_mysql_rolls_back_when_wanted_cleanup_fails(self):
        def execute_hook(sql, _params):
            if sql.startswith("DELETE FROM wanted"):
                raise self.module.mysql.connector.Error("delete failed")

        cursor = CursorStub(execute_hook=execute_hook, lastrowid=11)
        conn = ConnectionStub([cursor])

        with patch.object(self.module, "read_json_to_dict", return_value=build_movie_info()), patch.object(
            self.module, "create_conn", return_value=conn
        ), patch.object(
            self.module, "get_record_id_by_priority", return_value=None
        ), self.assertRaises(self.module.mysql.connector.Error):
            self.module.insert_movie_record_to_mysql("D:\\movies\\Broken")

        self.assertEqual(conn.commit_calls, 0)
        self.assertEqual(conn.rollback_calls, 1)
        self.assertTrue(conn.closed)

    def test_sort_movie_mysql_logs_and_reraises_when_movie_info_missing_required_fields(self):
        cursor = CursorStub()
        conn = ConnectionStub([cursor])
        movie_info = build_movie_info()
        del movie_info["director"]

        with patch.object(self.module, "read_json_to_dict", return_value=movie_info), patch.object(
            self.module, "create_conn", return_value=conn
        ), patch.object(
            self.module, "get_record_id_by_priority", return_value=None
        ), patch.object(
            self.module.logger, "error"
        ) as mock_error, self.assertRaises(KeyError):
            self.module.insert_movie_record_to_mysql("D:\\movies\\Broken")

        self.assertEqual(conn.commit_calls, 0)
        self.assertEqual(conn.rollback_calls, 0)
        self.assertEqual(cursor.execute_calls, [])
        self.assertTrue(cursor.closed)
        self.assertTrue(conn.closed)
        mock_error.assert_called_with("电影信息字段缺失：movie_info.json5 缺少必要字段: director")


class TestLookupHelpers(unittest.TestCase):
    def setUp(self):
        self.module = load_sort_movie_mysql()

    def test_check_movie_ids_batches_queries_and_preserves_input_order(self):
        conn = ConnectionStub([CursorStub()])

        with patch.object(self.module, "create_conn", return_value=conn), patch.object(
            self.module,
            "fetch_existing_values",
            side_effect=[{"tt1111111"}, {"200"}, set()],
        ) as mock_fetch:
            result = self.module.check_movie_ids(["tmdb200", "tt1111111", "db300", "unknown", "tt9999999"])

        self.assertEqual(result, ["db300", "unknown", "tt9999999"])
        self.assertEqual(
            [call.args for call in mock_fetch.call_args_list],
            [
                (conn.cursors[0], "movies", "imdb", {"tt1111111", "tt9999999"}),
                (conn.cursors[0], "movies", "tmdb", {"200"}),
                (conn.cursors[0], "movies", "douban", {"300"}),
            ],
        )
        self.assertTrue(conn.cursors[0].closed)
        self.assertTrue(conn.closed)

    def test_query_imdb_title_directors_returns_director_rows(self):
        cursor = CursorStub(fetchall_results=[[{"director_id": "nm1", "director_name": "A"}]])
        conn = ConnectionStub([cursor])

        with patch.object(self.module, "create_conn", return_value=conn):
            result = self.module.query_imdb_title_directors("tt1234567")

        self.assertEqual(result, [{"director_id": "nm1", "director_name": "A"}])
        self.assertEqual(
            cursor.execute_calls[0],
            (self.module.IMDB_TITLE_DIRECTOR_NAMES_QUERY_SQL, ("tt1234567",)),
        )
        self.assertTrue(cursor.closed)
        self.assertTrue(conn.closed)

    def test_query_imdb_title_directors_returns_none_on_mysql_error(self):
        def execute_hook(_sql, _params):
            raise self.module.mysql.connector.Error("query failed")

        cursor = CursorStub(execute_hook=execute_hook)
        conn = ConnectionStub([cursor])

        with patch.object(self.module, "create_conn", return_value=conn), patch.object(
            self.module.logger, "error"
        ) as mock_error:
            result = self.module.query_imdb_title_directors("tt7654321")

        self.assertIsNone(result)
        mock_error.assert_called_with("IMDb 本地库查询失败！tt7654321 query failed")
        self.assertTrue(conn.closed)

    def test_query_imdb_title_metadata_returns_titles_genres_and_directors(self):
        cursor = CursorStub(
            fetchone_results=[
                {
                    "imdb_id": "tt1234567",
                    "primary_title": "Primary Title",
                    "original_title": "Original Title",
                    "start_year": 1999,
                    "runtime_minutes": 123,
                    "title_type": "movie",
                    "genres": "Drama,Mystery,Drama",
                }
            ],
            fetchall_results=[
                [
                    {"director_order": 1, "director_id": "nm1", "director_name": "Director One"},
                    {"director_order": 2, "director_id": "nm2", "director_name": "Director Two"},
                    {"director_order": 3, "director_id": "nm2", "director_name": "Director Two"},
                ],
                [
                    {"ordering": 1, "title": "Original Title", "region": "US", "language": "en", "types": "original", "attributes": None, "is_original_title": 1},
                    {"ordering": 2, "title": "Alias A", "region": "JP", "language": "ja", "types": "imdbDisplay", "attributes": None, "is_original_title": 0},
                    {"ordering": 3, "title": "Alias B", "region": None, "language": None, "types": None, "attributes": None, "is_original_title": 0},
                    {"ordering": 4, "title": "Alias A", "region": None, "language": None, "types": None, "attributes": None, "is_original_title": 0},
                ],
            ],
        )
        conn = ConnectionStub([cursor])

        with patch.object(self.module, "create_conn", return_value=conn):
            result = self.module.query_imdb_title_metadata("tt1234567")

        self.assertEqual(
            result,
            {
                "imdb_id": "tt1234567",
                "primary_title": "Primary Title",
                "original_title": "Original Title",
                "start_year": 1999,
                "runtime_minutes": 123,
                "title_type": "movie",
                "genres": ["Drama", "Mystery"],
                "titles": ["Original Title", "Primary Title", "Alias A", "Alias B"],
                "directors": ["Director One", "Director Two"],
            },
        )
        self.assertEqual(
            cursor.execute_calls,
            [
                (self.module.IMDB_TITLE_BASICS_QUERY_SQL, ("tt1234567",)),
                (self.module.IMDB_TITLE_DIRECTORS_QUERY_SQL, ("tt1234567",)),
                (self.module.IMDB_TITLE_AKAS_QUERY_SQL, ("tt1234567",)),
            ],
        )
        self.assertEqual(conn.cursor_calls, [{"dictionary": True}])
        self.assertTrue(cursor.closed)
        self.assertTrue(conn.closed)

    def test_query_imdb_title_metadata_returns_none_on_mysql_error(self):
        def execute_hook(_sql, _params):
            raise self.module.mysql.connector.Error("query failed")

        cursor = CursorStub(execute_hook=execute_hook)
        conn = ConnectionStub([cursor])

        with patch.object(self.module, "create_conn", return_value=conn), patch.object(
            self.module.logger, "error"
        ) as mock_error:
            result = self.module.query_imdb_title_metadata("tt7654321")

        self.assertIsNone(result)
        mock_error.assert_called_with("IMDb 本地库查询影片失败！tt7654321 query failed")
        self.assertTrue(cursor.closed)
        self.assertTrue(conn.closed)

    def test_get_record_id_by_priority_prefers_imdb_match(self):
        cursor = CursorStub(fetchone_results=[(7,)])
        merged_dict = build_movie_info(tmdb="22", douban="33", imdb="tt1111111")

        result = self.module.get_record_id_by_priority(cursor, merged_dict)

        self.assertEqual(result, 7)
        self.assertEqual(cursor.execute_calls, [("SELECT id FROM movies WHERE imdb = %s", ("tt1111111",))])

    def test_get_record_id_by_priority_falls_back_to_tmdb_when_imdb_not_found(self):
        cursor = CursorStub(fetchone_results=[None, (9,)])
        merged_dict = build_movie_info(tmdb="22", douban="33", imdb="tt1111111")

        result = self.module.get_record_id_by_priority(cursor, merged_dict)

        self.assertEqual(result, 9)
        self.assertEqual(len(cursor.execute_calls), 2)
        self.assertEqual(
            cursor.execute_calls[-1],
            ("SELECT id FROM movies WHERE tmdb = %s", ("22",)),
        )

    def test_get_record_id_by_priority_returns_none_when_no_external_id_matches(self):
        cursor = CursorStub(fetchone_results=[None, None, None])
        merged_dict = build_movie_info(tmdb="22", douban="33", imdb="tt1111111")

        result = self.module.get_record_id_by_priority(cursor, merged_dict)

        self.assertIsNone(result)
        self.assertEqual(len(cursor.execute_calls), 3)


class TestDeleteHelpers(unittest.TestCase):
    def setUp(self):
        self.module = load_sort_movie_mysql()

    def test_remove_existing_tmdb_ids_returns_early_for_empty_set(self):
        with patch.object(self.module, "create_conn") as mock_create_conn:
            result = self.module.remove_existing_tmdb_ids(set())

        self.assertEqual(result, set())
        mock_create_conn.assert_not_called()

    def test_remove_existing_tmdb_ids_removes_found_values(self):
        cursor = CursorStub(fetchall_results=[[("101",)], [("303",)]])
        conn = ConnectionStub([cursor])

        with patch.object(self.module, "create_conn", return_value=conn):
            result = self.module.remove_existing_tmdb_ids({"101", "202", "303"})

        self.assertEqual(result, {"202"})
        self.assertEqual(len(cursor.execute_calls), 2)
        self.assertIn("SELECT tmdb FROM movies WHERE tmdb IN", cursor.execute_calls[0][0])
        self.assertIn("SELECT tmdb FROM wanted WHERE tmdb IN", cursor.execute_calls[1][0])
        self.assertTrue(cursor.closed)
        self.assertTrue(conn.closed)

    def test_remove_existing_tmdb_ids_closes_resources_when_query_fails(self):
        def execute_hook(_sql, _params):
            raise self.module.mysql.connector.Error("query failed")

        cursor = CursorStub(execute_hook=execute_hook)
        conn = ConnectionStub([cursor])

        with patch.object(self.module, "create_conn", return_value=conn):
            with self.assertRaises(self.module.mysql.connector.Error):
                self.module.remove_existing_tmdb_ids({"101"})

        self.assertTrue(cursor.closed)
        self.assertTrue(conn.closed)

    def test_delete_records_skips_when_no_valid_ids(self):
        with patch.object(self.module, "create_conn") as mock_create_conn, patch.object(
            self.module.logger, "info"
        ) as mock_info:
            self.module.delete_records([None, ""], "tmdb", "wanted")

        mock_create_conn.assert_not_called()
        mock_info.assert_called_with("wanted 表无有效的删除编号，跳过操作")

    def test_delete_records_executes_delete_for_filtered_ids(self):
        cursor = CursorStub()
        conn = ConnectionStub([cursor])

        with patch.object(self.module, "create_conn", return_value=conn), patch.object(
            self.module.logger, "info"
        ) as mock_info:
            self.module.delete_records([None, "tt1", "", "tt2"], "imdb", "movies")

        self.assertEqual(conn.commit_calls, 1)
        self.assertEqual(
            cursor.executemany_calls[0],
            ("DELETE FROM movies WHERE imdb = %s", [("tt1",), ("tt2",)]),
        )
        mock_info.assert_called_with("成功删除 movies 表中 2 条数据")

    def test_delete_records_rolls_back_and_closes_cursor_on_mysql_error(self):
        def executemany_hook(_sql, _params_list):
            raise self.module.mysql.connector.Error("delete failed")

        cursor = CursorStub(executemany_hook=executemany_hook)
        conn = ConnectionStub([cursor])

        with patch.object(self.module, "create_conn", return_value=conn), patch.object(
            self.module.logger, "error"
        ) as mock_error, self.assertRaises(self.module.mysql.connector.Error):
            self.module.delete_records(["tt1"], "imdb", "movies")

        self.assertEqual(conn.rollback_calls, 1)
        self.assertEqual(conn.commit_calls, 0)
        self.assertTrue(cursor.closed)
        self.assertTrue(conn.closed)
        mock_error.assert_called_with("操作异常：delete failed")


if __name__ == "__main__":
    unittest.main()
