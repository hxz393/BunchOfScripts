"""
针对 ``my_scripts.sort_movie_auto`` 的定向单元测试。

这些用例先把当前已知的高风险问题固化下来：
1. 下游步骤失败时，不应提前打平目录结构。
2. 豆瓣页面解析异常时，不应直接退出整个进程。

前 2 个问题当前生产代码尚未修复，因此这些用例先标记为 ``expectedFailure``。
后续修复对应问题后，移除装饰器即可把它们转成正式回归测试。
"""

import importlib.util
import json
import os
import re
import shutil
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "sort_movie_auto.py"


def fake_write_dict_to_json(target_path: str | os.PathLike, content: dict) -> bool:
    """以 JSON 文本模拟写入 ``movie_info.json5``。"""
    path = Path(target_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(content, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def fake_sanitize_filename(name: str) -> str:
    """测试中保留输入名称，避免引入无关逻辑。"""
    return name


def fake_extract_imdb_id(text: str) -> str | None:
    """从文本中提取首个 ``tt...``。"""
    match = re.search(r"tt\d+", text)
    return match.group(0) if match else None


def fake_safe_get(data: object, keys: list[object], default=None):
    """最小可用的嵌套字典读取辅助函数。"""
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key, default)
        elif isinstance(current, list) and isinstance(key, int) and 0 <= key < len(current):
            current = current[key]
        else:
            return default
        if current is None:
            return default
    return current


def fake_move_all_files_to_root(dir_path: str) -> None:
    """模拟当前打平目录逻辑，用于暴露提前变更目录结构的问题。"""
    root_path = Path(dir_path).resolve()

    for current_root, _dirs, files in os.walk(root_path):
        current_path = Path(current_root)
        if current_path == root_path:
            continue

        for file_name in files:
            source = current_path / file_name
            target = root_path / file_name
            if target.exists():
                base = target.stem
                suffix = target.suffix
                index = 1
                while True:
                    candidate = root_path / f"{base}({index}){suffix}"
                    if not candidate.exists():
                        target = candidate
                        break
                    index += 1
            shutil.move(str(source), str(target))

    for current_root, _dirs, _files in os.walk(root_path, topdown=False):
        current_path = Path(current_root)
        if current_path != root_path and not any(current_path.iterdir()):
            current_path.rmdir()


def load_sort_movie_auto():
    """在隔离依赖的环境中加载 ``sort_movie_auto`` 模块。"""
    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.sanitize_filename = fake_sanitize_filename
    fake_my_module.write_dict_to_json = fake_write_dict_to_json

    fake_sort_movie_mysql = types.ModuleType("sort_movie_mysql")
    fake_sort_movie_mysql.insert_movie_record_to_mysql = lambda _path: None
    fake_sort_movie_mysql.query_imdb_title_metadata = lambda _movie_id: None

    fake_sort_movie_ops = types.ModuleType("sort_movie_ops")
    fake_sort_movie_ops.move_all_files_to_root = fake_move_all_files_to_root
    fake_sort_movie_ops.extract_imdb_id = fake_extract_imdb_id
    fake_sort_movie_ops.get_dl_link = lambda _path: ""
    fake_sort_movie_ops.scan_ids = lambda _directory: {"tmdb": None, "douban": None, "imdb": None}
    fake_sort_movie_ops.remove_duplicates_ignore_case = lambda items: list(dict.fromkeys(items))
    fake_sort_movie_ops.safe_get = fake_safe_get
    fake_sort_movie_ops.build_movie_folder_name = lambda _path, _movie_dict: "Renamed Movie"
    fake_sort_movie_ops.merged_dict = lambda _path, _movie_info, movie_ids, file_info: movie_ids | file_info
    fake_sort_movie_ops.create_aka_movie = lambda _new_path, _movie_dict: None
    fake_sort_movie_ops.get_video_info = lambda _path: None
    fake_sort_movie_ops.check_movie = lambda _path: None
    fake_sort_movie_ops.get_movie_id = lambda movie_dict: movie_dict.get("imdb") or "noid"
    fake_sort_movie_ops.fix_douban_name = lambda text: text.strip()

    fake_sort_movie_request = types.ModuleType("sort_movie_request")
    fake_sort_movie_request.get_tmdb_search_response = lambda _search_id: {}
    fake_sort_movie_request.get_douban_response = lambda _query, _mode: None
    fake_sort_movie_request.get_douban_search_details = lambda _response: None
    fake_sort_movie_request.get_tmdb_movie_details = lambda _movie_id, _tv=False: {}
    fake_sort_movie_request.get_tmdb_movie_cover = lambda _poster_path, _image_path: None

    spec = importlib.util.spec_from_file_location(
        f"sort_movie_auto_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "retrying": fake_retrying,
            "my_module": fake_my_module,
            "sort_movie_mysql": fake_sort_movie_mysql,
            "sort_movie_ops": fake_sort_movie_ops,
            "sort_movie_request": fake_sort_movie_request,
        },
    ):
        spec.loader.exec_module(module)

    return module


class TestSortMovieAutoKnownRegressions(unittest.TestCase):
    """锁定 ``sort_movie_auto`` 当前已知的失败隔离问题。"""

    def setUp(self):
        self.module = load_sort_movie_auto()
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()

    @unittest.expectedFailure
    def test_sort_movie_auto_preserves_nested_files_when_follow_up_step_fails(self):
        """下游步骤失败时，不应先打平子目录。"""
        director_dir = Path(self.temp_dir.name) / "Director Name"
        movie_dir = director_dir / "Movie Title [tt1234567]"
        nested_dir = movie_dir / "disc1"
        nested_dir.mkdir(parents=True)
        nested_file = nested_dir / "movie.mkv"
        nested_file.write_text("video", encoding="utf-8")
        def fake_prepare_movie_folder_markers(path: str) -> None:
            return None

        with patch.object(
            self.module,
            "prepare_movie_folder_markers",
            side_effect=fake_prepare_movie_folder_markers,
        ), patch.object(
            self.module,
            "sort_movie",
            side_effect=RuntimeError("metadata lookup failed"),
        ):
            with self.assertRaises(RuntimeError):
                self.module.sort_movie_auto(str(director_dir))

        self.assertTrue(nested_file.exists())

    @unittest.expectedFailure
    def test_get_douban_movie_info_does_not_exit_process_on_unexpected_html(self):
        """豆瓣页面结构异常时，应作为单条失败返回，而不是退出整个进程。"""
        response = Mock(text="<html><title>anti bot</title></html>")
        movie_info = {
            "original_title": "",
            "chinese_title": "",
            "titles": [],
            "directors": [],
            "genres": [],
            "country": [],
            "language": [],
            "year": 0,
            "runtime": 0,
        }

        with patch.object(self.module, "get_douban_response", return_value=response):
            self.module.get_douban_movie_info("123456", movie_info)

    def test_sort_movie_rolls_back_when_validation_fails(self):
        """校验失败时，应回滚到整理前状态。"""
        movie_dir = Path(self.temp_dir.name) / "Movie Folder"
        movie_dir.mkdir()

        movie_ids = {"tmdb": None, "douban": None, "imdb": "tt1234567"}
        file_info = {
            "source": "BluRay",
            "resolution": "1080p",
            "codec": "h264",
            "bitrate": "8000kbps",
            "duration": 120,
            "quality": "1080p",
        }
        movie_dict = movie_ids | file_info | {"poster_path": "/poster.jpg"}

        def fake_download_cover(_poster_path: str, image_path: str) -> None:
            Path(image_path).write_text("poster", encoding="utf-8")

        def fake_create_aka_movie(new_path: str, _movie_dict: dict) -> None:
            Path(new_path, "Alias Title.别名").write_text("", encoding="utf-8")

        with patch.object(self.module, "scan_ids", return_value=movie_ids), patch.object(
            self.module,
            "get_imdb_movie_info",
        ), patch.object(
            self.module,
            "get_video_info",
            return_value=file_info,
        ), patch.object(
            self.module,
            "merged_dict",
            return_value=movie_dict,
        ), patch.object(
            self.module,
            "build_movie_folder_name",
            return_value="Renamed Movie",
        ), patch.object(
            self.module,
            "get_movie_id",
            return_value="tt1234567",
        ), patch.object(
            self.module,
            "check_movie",
            return_value="validation failed",
        ), patch.object(
            self.module,
            "get_tmdb_movie_cover",
            side_effect=fake_download_cover,
        ), patch.object(
            self.module,
            "create_aka_movie",
            side_effect=fake_create_aka_movie,
        ), patch.object(
            self.module,
            "insert_movie_record_to_mysql",
        ) as mock_insert:
            self.module.sort_movie(str(movie_dir))

        renamed_dir = movie_dir.parent / "Renamed Movie"
        self.assertTrue(movie_dir.exists())
        self.assertFalse(renamed_dir.exists())
        self.assertFalse((movie_dir / "tt1234567.jpg").exists())
        self.assertFalse((movie_dir / "Alias Title.别名").exists())
        self.assertFalse((movie_dir / "movie_info.json5").exists())
        mock_insert.assert_not_called()

    def test_sort_movie_restores_existing_movie_info_when_validation_fails(self):
        """校验失败时，已有 ``movie_info.json5`` 应恢复为整理前内容。"""
        movie_dir = Path(self.temp_dir.name) / "Movie Folder With Json"
        movie_dir.mkdir()
        original_bytes = b'{"original": true}'
        (movie_dir / "movie_info.json5").write_bytes(original_bytes)

        movie_ids = {"tmdb": None, "douban": None, "imdb": "tt1234567"}
        file_info = {
            "source": "BluRay",
            "resolution": "1080p",
            "codec": "h264",
            "bitrate": "8000kbps",
            "duration": 120,
            "quality": "1080p",
        }
        movie_dict = movie_ids | file_info | {"poster_path": "/poster.jpg"}

        with patch.object(self.module, "scan_ids", return_value=movie_ids), patch.object(
            self.module,
            "get_imdb_movie_info",
        ), patch.object(
            self.module,
            "get_video_info",
            return_value=file_info,
        ), patch.object(
            self.module,
            "merged_dict",
            return_value=movie_dict,
        ), patch.object(
            self.module,
            "build_movie_folder_name",
            return_value="Renamed Movie",
        ), patch.object(
            self.module,
            "get_movie_id",
            return_value="tt1234567",
        ), patch.object(
            self.module,
            "check_movie",
            return_value="validation failed",
        ):
            self.module.sort_movie(str(movie_dir))

        self.assertTrue(movie_dir.exists())
        self.assertEqual((movie_dir / "movie_info.json5").read_bytes(), original_bytes)

    def test_sort_movie_rolls_back_when_transaction_steps_raise(self):
        """落盘事务中的异常都应触发回滚。"""
        movie_ids = {"tmdb": None, "douban": None, "imdb": "tt1234567"}
        file_info = {
            "source": "BluRay",
            "resolution": "1080p",
            "codec": "h264",
            "bitrate": "8000kbps",
            "duration": 120,
            "quality": "1080p",
        }
        movie_dict = movie_ids | file_info | {"poster_path": "/poster.jpg"}

        def make_movie_dir(name: str) -> Path:
            movie_dir = Path(self.temp_dir.name) / name
            movie_dir.mkdir()
            return movie_dir

        def cover_then_raise(_poster_path: str, image_path: str) -> None:
            Path(image_path).write_text("poster", encoding="utf-8")
            raise RuntimeError("cover failed")

        def alias_then_raise(new_path: str, _movie_dict: dict) -> None:
            Path(new_path, "Alias Title.别名").write_text("", encoding="utf-8")
            raise RuntimeError("alias failed")

        def json_then_raise(target_path: str, content: dict) -> None:
            fake_write_dict_to_json(target_path, content)
            raise RuntimeError("json failed")

        cases = [
            ("cover", "get_tmdb_movie_cover", cover_then_raise),
            ("alias", "create_aka_movie", alias_then_raise),
            ("json", "write_dict_to_json", json_then_raise),
            ("insert", "insert_movie_record_to_mysql", RuntimeError("insert failed")),
        ]

        for case_name, patch_target, side_effect in cases:
            with self.subTest(case=case_name):
                movie_dir = make_movie_dir(f"Movie Folder {case_name}")

                with patch.object(self.module, "scan_ids", return_value=movie_ids), patch.object(
                    self.module,
                    "get_imdb_movie_info",
                ), patch.object(
                    self.module,
                    "get_video_info",
                    return_value=file_info,
                ), patch.object(
                    self.module,
                    "merged_dict",
                    return_value=movie_dict,
                ), patch.object(
                    self.module,
                    "build_movie_folder_name",
                    return_value="Renamed Movie",
                ), patch.object(
                    self.module,
                    "get_movie_id",
                    return_value="tt1234567",
                ), patch.object(
                    self.module,
                    "check_movie",
                    return_value=None,
                ), patch.object(
                    self.module,
                    patch_target,
                    side_effect=side_effect,
                ):
                    self.module.sort_movie(str(movie_dir))

                renamed_dir = movie_dir.parent / "Renamed Movie"
                self.assertTrue(movie_dir.exists())
                self.assertFalse(renamed_dir.exists())
                self.assertFalse((movie_dir / "tt1234567.jpg").exists())
                self.assertFalse((movie_dir / "Alias Title.别名").exists())
                self.assertFalse((movie_dir / "movie_info.json5").exists())

    def test_sort_movie_auto_continues_after_one_folder_fails(self):
        """单个目录失败后，应移到检验目录并继续处理后续目录。"""
        director_dir = Path(self.temp_dir.name) / "Director Name"
        director_dir.mkdir()
        first_movie = director_dir / "01 Bad Movie [tt0000001]"
        second_movie = director_dir / "02 Good Movie [tt0000002]"
        first_movie.mkdir()
        second_movie.mkdir()
        quarantine_root = Path(self.temp_dir.name) / "quarantine"
        handled_paths: list[str] = []

        def fake_prepare_movie_folder_markers(path: str) -> tuple[str, str] | None:
            handled_paths.append(path)
            if path.endswith("01 Bad Movie [tt0000001]"):
                return "missing_supported_folder_id", "missing imdb"
            return None

        with patch.object(
            self.module.os,
            "listdir",
            return_value=[first_movie.name, second_movie.name],
        ), patch.object(
            self.module,
            "FAILED_MOVIE_ROOT",
            str(quarantine_root),
        ), patch.object(
            self.module,
            "prepare_movie_folder_markers",
            side_effect=fake_prepare_movie_folder_markers,
        ), patch.object(
            self.module,
            "move_all_files_to_root",
        ), patch.object(
            self.module,
            "sort_movie",
        ) as mock_sort_movie:
            self.module.sort_movie_auto(str(director_dir))

        self.assertEqual(handled_paths, [str(first_movie), str(second_movie)])
        mock_sort_movie.assert_called_once_with(str(second_movie))
        self.assertFalse(first_movie.exists())
        self.assertTrue((quarantine_root / "Director Name" / first_movie.name).exists())


class TestSortMovieAutoCurrentRules(unittest.TestCase):
    """验证当前已生效的整理准入规则。"""

    def setUp(self):
        self.module = load_sort_movie_auto()
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_sort_movie_rejects_directory_without_any_ids_even_if_movie_info_exists(self):
        """只要没有编号文件，就不应继续处理或入库。"""
        movie_dir = Path(self.temp_dir.name) / "Movie Folder"
        movie_dir.mkdir()
        (movie_dir / "movie_info.json5").write_text("{}", encoding="utf-8")

        with patch.object(
            self.module,
            "scan_ids",
            return_value={"tmdb": None, "douban": None, "imdb": None},
        ), patch.object(
            self.module,
            "get_tmdb_movie_info",
        ) as mock_tmdb, patch.object(
            self.module,
            "get_imdb_movie_info",
        ) as mock_imdb, patch.object(
            self.module,
            "get_douban_movie_info",
        ) as mock_douban, patch.object(
            self.module,
            "get_video_info",
        ) as mock_video_info, patch.object(
            self.module,
            "insert_movie_record_to_mysql",
        ) as mock_insert:
            self.module.sort_movie(str(movie_dir))

        mock_tmdb.assert_not_called()
        mock_imdb.assert_not_called()
        mock_douban.assert_not_called()
        mock_video_info.assert_not_called()
        mock_insert.assert_not_called()

    def test_sort_movie_stops_cleanly_when_target_directory_already_exists(self):
        """目标目录已存在时，应直接停止当前整理，不抛异常也不改动原目录。"""
        movie_dir = Path(self.temp_dir.name) / "Movie Folder"
        movie_dir.mkdir()
        existing_target = movie_dir.parent / "Renamed Movie"
        existing_target.mkdir()

        movie_ids = {"tmdb": None, "douban": None, "imdb": "tt1234567"}
        file_info = {
            "source": "BluRay",
            "resolution": "1080p",
            "codec": "h264",
            "bitrate": "8000kbps",
            "duration": 120,
            "quality": "1080p",
        }
        movie_dict = movie_ids | file_info | {"poster_path": "/poster.jpg"}

        with patch.object(self.module, "scan_ids", return_value=movie_ids), patch.object(
            self.module,
            "get_imdb_movie_info",
        ), patch.object(
            self.module,
            "get_video_info",
            return_value=file_info,
        ), patch.object(
            self.module,
            "merged_dict",
            return_value=movie_dict,
        ), patch.object(
            self.module,
            "build_movie_folder_name",
            return_value="Renamed Movie",
        ), patch.object(
            self.module,
            "get_tmdb_movie_cover",
        ) as mock_cover, patch.object(
            self.module,
            "create_aka_movie",
        ) as mock_aliases, patch.object(
            self.module,
            "write_dict_to_json",
        ) as mock_write_json, patch.object(
            self.module,
            "insert_movie_record_to_mysql",
        ) as mock_insert:
            self.module.sort_movie(str(movie_dir))

        self.assertTrue(movie_dir.exists())
        self.assertTrue(existing_target.exists())
        mock_cover.assert_not_called()
        mock_aliases.assert_not_called()
        mock_write_json.assert_not_called()
        mock_insert.assert_not_called()

    def test_sort_movie_uses_tmdb_tv_suffix_to_enable_tv_mode(self):
        """只有 TMDB 编号以 ``tv`` 结尾时，才应按电视剧模式查询详情。"""
        movie_dir = Path(self.temp_dir.name) / "Movie Folder"
        movie_dir.mkdir()

        with patch.object(
            self.module,
            "scan_ids",
            return_value={"tmdb": "12345tv", "douban": None, "imdb": None},
        ), patch.object(
            self.module,
            "get_tmdb_movie_info",
        ) as mock_tmdb, patch.object(
            self.module,
            "get_video_info",
            return_value=None,
        ):
            self.module.sort_movie(str(movie_dir))

        mock_tmdb.assert_called_once()
        self.assertEqual(mock_tmdb.call_args.args[0], "12345")
        self.assertTrue(mock_tmdb.call_args.args[2])


class TestSortMovieAutoFolderEntrypoints(unittest.TestCase):
    """验证目录名编号如何决定 ``sort_movie_auto`` 的入口分支。"""

    def setUp(self):
        self.module = load_sort_movie_auto()
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_prepare_movie_folder_markers_uses_imdb_lookup_when_folder_name_has_tt_id(self):
        """目录名带 ``tt...`` 时，应直接创建 ``.imdb/.tmdb/.douban`` 空文件。"""
        movie_dir = Path(self.temp_dir.name) / "2023 - Movie Name {tt1234567}"
        movie_dir.mkdir()

        with patch.object(
            self.module,
            "query_imdb_title_metadata",
            return_value={"imdb_id": "tt1234567"},
        ), patch.object(
            self.module,
            "get_tmdb_id",
            return_value={"result": "", "tmdb_id": "321"},
        ) as mock_tmdb, patch.object(
            self.module,
            "get_douban_id",
            return_value={"result": "", "douban_id": "654321"},
        ) as mock_douban:
            result = self.module.prepare_movie_folder_markers(str(movie_dir))

        self.assertIsNone(result)
        self.assertTrue((movie_dir / "tt1234567.imdb").exists())
        self.assertTrue((movie_dir / "321.tmdb").exists())
        self.assertTrue((movie_dir / "654321.douban").exists())
        mock_tmdb.assert_called_once_with("tt1234567")
        mock_douban.assert_called_once_with("tt1234567")

    def test_prepare_movie_folder_markers_rejects_duplicate_tmdb_marker_files(self):
        """同类型编号空文件出现多个时，应直接报清理错误。"""
        movie_dir = Path(self.temp_dir.name) / "2023 - Movie Name {tt1234567}"
        movie_dir.mkdir()
        (movie_dir / "321.tmdb").touch()
        (movie_dir / "654.tmdb").touch()

        result = self.module.prepare_movie_folder_markers(str(movie_dir))

        self.assertEqual(result[0], "duplicate_marker_files")
        self.assertIn("TMDB 编号文件太多", result[1])

    def test_sort_movie_auto_moves_folder_when_imdb_id_is_outdated(self):
        """本地 IMDb 查不到旧 ``tt`` 时，应删掉错误 ``.imdb`` 并移到检验目录。"""
        root_dir = Path(self.temp_dir.name) / "Director Name"
        movie_dir = root_dir / "2014 - The Grand Budapest Hotel {tt2404467}"
        movie_dir.mkdir(parents=True)
        (movie_dir / "tt2404467.imdb").touch()
        quarantine_root = Path(self.temp_dir.name) / "quarantine"

        with patch.object(
            self.module,
            "FAILED_MOVIE_ROOT",
            str(quarantine_root),
        ), patch.object(
            self.module,
            "query_imdb_title_metadata",
            return_value=None,
        ), patch.object(
            self.module,
            "move_all_files_to_root",
        ) as mock_move, patch.object(
            self.module,
            "sort_movie",
        ) as mock_sort_movie:
            self.module.sort_movie_auto(str(root_dir))

        moved_dir = quarantine_root / "Director Name" / movie_dir.name
        self.assertTrue(moved_dir.exists())
        self.assertFalse((moved_dir / "tt2404467.imdb").exists())
        mock_move.assert_not_called()
        mock_sort_movie.assert_not_called()

    def test_prepare_movie_folder_markers_keeps_only_imdb_when_auto_lookups_miss(self):
        """IMDb 有效但 TMDB/Douban 都没补到时，应保留 ``.imdb`` 并继续。"""
        movie_dir = Path(self.temp_dir.name) / "2023 - Movie Name {tt1234567}"
        movie_dir.mkdir()

        with patch.object(
            self.module,
            "query_imdb_title_metadata",
            return_value={"imdb_id": "tt1234567"},
        ), patch.object(
            self.module,
            "get_tmdb_id",
            return_value={"result": "tmdb miss", "tmdb_id": ""},
        ), patch.object(
            self.module,
            "get_douban_id",
            return_value={"result": "", "douban_id": ""},
        ):
            result = self.module.prepare_movie_folder_markers(str(movie_dir))

        self.assertIsNone(result)
        self.assertTrue((movie_dir / "tt1234567.imdb").exists())
        self.assertFalse((movie_dir / "321.tmdb").exists())
        self.assertFalse((movie_dir / "654321.douban").exists())

    def test_prepare_movie_folder_markers_accepts_tmdb_folder_name_with_verified_douban_marker(self):
        """目录名只有 ``tmdb...`` 时，只要已有任一非 IMDb 空文件就允许直达抓取步骤。"""
        movie_dir = Path(self.temp_dir.name) / "1967 - The Fiend with the Electronic Brain {tmdb699177}"
        movie_dir.mkdir()
        (movie_dir / "790434.douban").touch()

        result = self.module.prepare_movie_folder_markers(str(movie_dir))

        self.assertIsNone(result)

    def test_prepare_movie_folder_markers_accepts_douban_folder_name_with_verified_tmdb_marker(self):
        """目录名只有 ``db...`` 时，只要已有任一非 IMDb 空文件就允许直达抓取步骤。"""
        movie_dir = Path(self.temp_dir.name) / "2007 - The Casting db790434"
        movie_dir.mkdir()
        (movie_dir / "699177.tmdb").touch()

        result = self.module.prepare_movie_folder_markers(str(movie_dir))

        self.assertIsNone(result)

    def test_prepare_movie_folder_markers_rejects_manual_ids_without_verified_marker_files(self):
        """目录名只有手工 ``tmdb/db`` 编号时，缺少非 IMDb 空文件就必须跳过。"""
        movie_dir = Path(self.temp_dir.name) / "2007 - The Casting db790434"
        movie_dir.mkdir()

        result = self.module.prepare_movie_folder_markers(str(movie_dir))

        self.assertEqual(result[0], "missing_verified_manual_marker")
        self.assertIn(".tmdb/.douban", result[1])

    def test_prepare_movie_folder_markers_rejects_same_type_marker_conflict(self):
        """目录名编号与同类型空文件不一致时，应直接报冲突。"""
        movie_dir = Path(self.temp_dir.name) / "1967 - Movie Name {tmdb699177}"
        movie_dir.mkdir()
        (movie_dir / "123456.tmdb").touch()

        result = self.module.prepare_movie_folder_markers(str(movie_dir))

        self.assertEqual(result[0], "folder_name_marker_conflict")
        self.assertIn("TMDB 编号与空文件不一致", result[1])

    def test_prepare_movie_folder_markers_rejects_folder_name_without_supported_ids(self):
        """目录名里没有任何 ``tt/tmdb/db`` 编号时，应直接跳过。"""
        movie_dir = Path(self.temp_dir.name) / "Movie Without Any ID"
        movie_dir.mkdir()

        result = self.module.prepare_movie_folder_markers(str(movie_dir))

        self.assertEqual(result[0], "missing_supported_folder_id")
        self.assertIn("目录名缺少受支持的电影编号", result[1])

    def test_prepare_movie_folder_markers_rejects_short_tmdb_token_as_noise(self):
        """过短的 ``tmdb`` 数字串不应被当成有效目录编号。"""
        movie_dir = Path(self.temp_dir.name) / "Movie tmdb1"
        movie_dir.mkdir()

        result = self.module.prepare_movie_folder_markers(str(movie_dir))

        self.assertEqual(result[0], "missing_supported_folder_id")
        self.assertIn("目录名缺少受支持的电影编号", result[1])

    def test_prepare_movie_folder_markers_rejects_short_douban_token_as_noise(self):
        """过短的 ``db`` 数字串不应被当成有效目录编号。"""
        movie_dir = Path(self.temp_dir.name) / "Movie db123"
        movie_dir.mkdir()

        result = self.module.prepare_movie_folder_markers(str(movie_dir))

        self.assertEqual(result[0], "missing_supported_folder_id")
        self.assertIn("目录名缺少受支持的电影编号", result[1])

    def test_sort_movie_auto_processes_verified_tmdb_named_folder_without_id_lookup_step(self):
        """手工 ``tmdb/db`` 目录应直接进入抓取信息步骤。"""
        root_dir = Path(self.temp_dir.name) / "Director Name"
        movie_dir = root_dir / "2007 - The Casting db790434"
        movie_dir.mkdir(parents=True)
        (movie_dir / "790434.douban").touch()

        with patch.object(
            self.module,
            "move_all_files_to_root",
        ) as mock_move, patch.object(
            self.module,
            "sort_movie",
        ) as mock_sort_movie:
            self.module.sort_movie_auto(str(root_dir))

        mock_move.assert_called_once_with(str(movie_dir))
        mock_sort_movie.assert_called_once_with(str(movie_dir))

    def test_sort_movie_auto_creates_markers_for_imdb_named_folder_before_sorting(self):
        """目录名带 ``tt...`` 时，应先创建编号空文件再抓取信息。"""
        root_dir = Path(self.temp_dir.name) / "Director Name"
        movie_dir = root_dir / "2023 - Movie Name {tt1234567}"
        movie_dir.mkdir(parents=True)

        with patch.object(
            self.module,
            "query_imdb_title_metadata",
            return_value={"imdb_id": "tt1234567"},
        ), patch.object(
            self.module,
            "get_tmdb_id",
            return_value={"result": "", "tmdb_id": "321"},
        ), patch.object(
            self.module,
            "get_douban_id",
            return_value={"result": "", "douban_id": "654321"},
        ), patch.object(
            self.module,
            "move_all_files_to_root",
        ) as mock_move, patch.object(
            self.module,
            "sort_movie",
        ) as mock_sort_movie:
            self.module.sort_movie_auto(str(root_dir))

        mock_move.assert_called_once_with(str(movie_dir))
        self.assertTrue((movie_dir / "tt1234567.imdb").exists())
        self.assertTrue((movie_dir / "321.tmdb").exists())
        self.assertTrue((movie_dir / "654321.douban").exists())
        mock_sort_movie.assert_called_once_with(str(movie_dir))

    def test_prepare_movie_folder_markers_rejects_auto_tmdb_conflict_with_existing_marker(self):
        """IMDb 自动补出的 TMDB 编号与现有空文件不一致时，应直接报错。"""
        movie_dir = Path(self.temp_dir.name) / "2023 - Movie Name {tt1234567}"
        movie_dir.mkdir()
        (movie_dir / "999.tmdb").touch()

        with patch.object(
            self.module,
            "query_imdb_title_metadata",
            return_value={"imdb_id": "tt1234567"},
        ), patch.object(
            self.module,
            "get_tmdb_id",
            return_value={"result": "", "tmdb_id": "321"},
        ), patch.object(
            self.module,
            "get_douban_id",
            return_value={"result": "", "douban_id": "654321"},
        ):
            result = self.module.prepare_movie_folder_markers(str(movie_dir))

        self.assertEqual(result[0], "auto_tmdb_marker_conflict")
        self.assertIn("IMDb 自动补出的 TMDB 编号与空文件不一致", result[1])

    def test_prepare_movie_folder_markers_rejects_auto_douban_conflict_with_existing_marker(self):
        """IMDb 自动补出的 Douban 编号与现有空文件不一致时，应直接报错。"""
        movie_dir = Path(self.temp_dir.name) / "2023 - Movie Name {tt1234567}"
        movie_dir.mkdir()
        (movie_dir / "111111.douban").touch()

        with patch.object(
            self.module,
            "query_imdb_title_metadata",
            return_value={"imdb_id": "tt1234567"},
        ), patch.object(
            self.module,
            "get_tmdb_id",
            return_value={"result": "", "tmdb_id": "321"},
        ), patch.object(
            self.module,
            "get_douban_id",
            return_value={"result": "", "douban_id": "654321"},
        ):
            result = self.module.prepare_movie_folder_markers(str(movie_dir))

        self.assertEqual(result[0], "auto_douban_marker_conflict")
        self.assertIn("IMDb 自动补出的 DOUBAN 编号与空文件不一致", result[1])

    def test_get_tmdb_id_marks_tv_results_with_tv_suffix(self):
        """TMDB find 命中电视剧结果时，应返回带 ``tv`` 后缀的编号。"""
        with patch.object(
            self.module,
            "get_tmdb_search_response",
            return_value={"movie_results": [], "tv_results": [{"id": 12345}]},
        ):
            result = self.module.get_tmdb_id("tt7654321")

        self.assertEqual(result, {"result": "", "tmdb_id": "12345tv"})

    def test_get_tmdb_id_prefers_movie_when_movie_and_tv_results_both_exist(self):
        """TMDB find 同时命中电影和电视剧时，应优先返回电影编号。"""
        with patch.object(
            self.module,
            "get_tmdb_search_response",
            return_value={"movie_results": [{"id": 888}], "tv_results": [{"id": 12345}]},
        ):
            result = self.module.get_tmdb_id("tt7654321")

        self.assertEqual(result, {"result": "", "tmdb_id": "888"})

    def test_get_douban_id_reports_search_failure(self):
        """豆瓣搜索请求失败时，应返回明确错误。"""
        with patch.object(self.module, "get_douban_response", return_value=None):
            result = self.module.get_douban_id("tt1234567")

        self.assertEqual(result, {"result": "豆瓣电影搜索失败", "douban_id": ""})

    def test_get_douban_id_returns_empty_when_search_details_missing(self):
        """豆瓣搜索页无法解析出唯一详情链接时，应安静返回空编号。"""
        with patch.object(self.module, "get_douban_response", return_value=object()), patch.object(
            self.module,
            "get_douban_search_details",
            return_value=None,
        ):
            result = self.module.get_douban_id("tt1234567")

        self.assertEqual(result, {"result": "", "douban_id": ""})

    def test_get_douban_id_reports_invalid_subject_link(self):
        """豆瓣详情链接不含 subject 数字编号时，应返回明确错误。"""
        with patch.object(self.module, "get_douban_response", return_value=object()), patch.object(
            self.module,
            "get_douban_search_details",
            return_value="https://movie.douban.com/celebrity/123456/",
        ):
            result = self.module.get_douban_id("tt1234567")

        self.assertEqual(result, {"result": "豆瓣链接里没有有效编号", "douban_id": ""})

    def test_get_douban_id_extracts_subject_digits(self):
        """豆瓣详情链接包含 subject 编号时，应正确提取纯数字 ID。"""
        with patch.object(self.module, "get_douban_response", return_value=object()), patch.object(
            self.module,
            "get_douban_search_details",
            return_value="https://movie.douban.com/subject/790434/",
        ):
            result = self.module.get_douban_id("tt1234567")

        self.assertEqual(result, {"result": "", "douban_id": "790434"})


class TestImdbLocalMerge(unittest.TestCase):
    """验证 IMDb 本地镜像结果如何合并到 ``movie_info``。"""

    def setUp(self):
        self.module = load_sort_movie_auto()

    def test_get_imdb_movie_info_uses_local_aliases_and_keeps_country_language_untouched(self):
        """本地 IMDb 应补充全量标题、类型和导演，但不覆盖其他站点提供的国家和语言。"""
        movie_info = {
            "year": 0,
            "runtime": 0,
            "runtime_imdb": 0,
            "original_title": "",
            "chinese_title": "",
            "titles": [],
            "genres": [],
            "country": ["Japan"],
            "language": ["Japanese"],
            "directors": [],
        }
        imdb_row = {
            "imdb_id": "tt1234567",
            "primary_title": "Primary Title",
            "original_title": "Original Title",
            "start_year": 1998,
            "runtime_minutes": 123,
            "title_type": "movie",
            "genres": ["Drama", "Mystery"],
            "titles": ["Original Title", "Primary Title", "Alias A", "Alias B"],
            "directors": ["Director One", "Director Two"],
        }

        with patch.object(self.module, "query_imdb_title_metadata", return_value=imdb_row):
            self.module.get_imdb_movie_info("tt1234567", movie_info)

        self.assertEqual(movie_info["year"], 1998)
        self.assertEqual(movie_info["runtime"], 123)
        self.assertEqual(movie_info["runtime_imdb"], 123)
        self.assertEqual(movie_info["original_title"], "Original Title")
        self.assertEqual(
            movie_info["titles"],
            ["Original Title", "Primary Title", "Alias A", "Alias B"],
        )
        self.assertEqual(movie_info["genres"], ["Drama", "Mystery"])
        self.assertEqual(movie_info["directors"], ["Director One", "Director Two"])
        self.assertEqual(movie_info["country"], ["Japan"])
        self.assertEqual(movie_info["language"], ["Japanese"])

    def test_get_imdb_movie_info_leaves_movie_info_unchanged_when_local_record_missing(self):
        """本地 IMDb 未命中时，应只报错并保持 ``movie_info`` 原值。"""
        movie_info = {
            "year": 1998,
            "runtime": 123,
            "runtime_imdb": 123,
            "original_title": "Original Title",
            "chinese_title": "中文名",
            "titles": ["Original Title"],
            "genres": ["Drama"],
            "country": ["Japan"],
            "language": ["Japanese"],
            "directors": ["Director One"],
        }
        before = json.loads(json.dumps(movie_info, ensure_ascii=False))

        with patch.object(self.module, "query_imdb_title_metadata", return_value=None):
            self.module.get_imdb_movie_info("tt1234567", movie_info)

        self.assertEqual(movie_info, before)


class TestTmdbMerge(unittest.TestCase):
    """验证 TMDB 电影和电视剧元数据如何合并到 ``movie_info``。"""

    def setUp(self):
        self.module = load_sort_movie_auto()

    def test_get_tmdb_movie_info_for_tv_uses_total_episode_count_and_tv_alt_titles(self):
        """TV 分支应优先用总集数估算时长，并读取 ``results`` 里的别名。"""
        movie_info = {
            "year": 0,
            "runtime": 0,
            "runtime_tmdb": 0,
            "original_title": "",
            "chinese_title": "",
            "titles": [],
            "genres": [],
            "country": [],
            "language": [],
            "directors": [],
            "poster_path": "",
        }
        tmdb_row = {
            "genres": [{"name": "Drama"}, {"name": "Fantasy"}],
            "origin_country": ["US"],
            "original_language": "en",
            "original_name": "Game of Thrones",
            "first_air_date": "2011-04-17",
            "last_episode_to_air": {"runtime": 80, "episode_number": 6},
            "number_of_episodes": 73,
            "credits": {
                "crew": [
                    {"known_for_department": "Directing", "original_name": "Alan Taylor", "name": "Alan Taylor"},
                ]
            },
            "created_by": [{"original_name": "David Benioff", "name": "David Benioff"}],
            "translations": {
                "translations": [
                    {"iso_3166_1": "CN", "data": {"name": "权力的游戏"}},
                    {"iso_3166_1": "US", "data": {"name": "Game of Thrones"}},
                ]
            },
            "results": [
                {"iso_3166_1": "AL", "title": "Froni i shpatave", "type": ""},
                {"iso_3166_1": "BR", "title": "A Guerra dos Tronos", "type": ""},
            ],
            "name": "Game of Thrones",
            "poster_path": "/poster.jpg",
        }

        with patch.object(self.module, "get_tmdb_movie_details", return_value=tmdb_row):
            self.module.get_tmdb_movie_info("1399", movie_info, tv=True)

        self.assertEqual(movie_info["year"], "2011")
        self.assertEqual(movie_info["runtime"], 80 * 73)
        self.assertEqual(movie_info["runtime_tmdb"], 80 * 73)
        self.assertEqual(movie_info["original_title"], "Game of Thrones")
        self.assertEqual(movie_info["chinese_title"], "权力的游戏")
        self.assertIn("Froni i shpatave", movie_info["titles"])
        self.assertIn("A Guerra dos Tronos", movie_info["titles"])
        self.assertIn("Alan Taylor", movie_info["directors"])
        self.assertIn("David Benioff", movie_info["directors"])

    def test_get_tmdb_movie_info_for_tv_falls_back_to_last_episode_number(self):
        """总集数不可用时，应回退到最后一集所在季的集号近似估算。"""
        movie_info = {
            "year": 0,
            "runtime": 0,
            "runtime_tmdb": 0,
            "original_title": "",
            "chinese_title": "",
            "titles": [],
            "genres": [],
            "country": [],
            "language": [],
            "directors": [],
            "poster_path": "",
        }
        tmdb_row = {
            "genres": [],
            "origin_country": ["US"],
            "original_language": "en",
            "original_name": "Mini Series",
            "first_air_date": "2020-01-01",
            "last_episode_to_air": {"runtime": 50, "episode_number": 4},
            "number_of_episodes": None,
            "credits": {"crew": []},
            "created_by": [],
            "translations": {"translations": []},
            "results": [],
            "name": "Mini Series",
            "poster_path": "/poster.jpg",
        }

        with patch.object(self.module, "get_tmdb_movie_details", return_value=tmdb_row):
            self.module.get_tmdb_movie_info("2000", movie_info, tv=True)

        self.assertEqual(movie_info["runtime"], 200)
        self.assertEqual(movie_info["runtime_tmdb"], 200)

    def test_get_tmdb_movie_info_returns_without_retry_when_tmdb_has_no_record(self):
        """TMDB 明确无记录时，应直接返回，不再由外层重复重试。"""
        movie_info = {
            "year": 0,
            "runtime": 0,
            "runtime_tmdb": 0,
            "original_title": "",
            "chinese_title": "",
            "titles": [],
            "genres": [],
            "country": [],
            "language": [],
            "directors": [],
            "poster_path": "",
        }

        with patch.object(self.module, "get_tmdb_movie_details", return_value=None):
            self.module.get_tmdb_movie_info("9999999", movie_info, tv=False)

        self.assertEqual(
            movie_info,
            {
                "year": 0,
                "runtime": 0,
                "runtime_tmdb": 0,
                "original_title": "",
                "chinese_title": "",
                "titles": [],
                "genres": [],
                "country": [],
                "language": [],
                "directors": [],
                "poster_path": "",
            },
        )


if __name__ == "__main__":
    unittest.main()
