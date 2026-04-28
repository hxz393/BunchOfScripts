"""
针对 ``my_scripts.sort_movie_auto`` 的定向单元测试。

这些用例覆盖自动整理入口、编号准入、失败隔离、回滚、元数据合并和站点解析规则。
"""

import importlib.util
import json
import os
import re
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


def fake_build_unique_path(target_path: str | os.PathLike) -> Path:
    """测试环境里的最小可用不重名路径生成。"""
    path = Path(target_path)
    if not path.exists():
        return path

    index = 1
    while True:
        candidate = path.with_name(f"{path.stem}({index}){path.suffix}")
        if not candidate.exists():
            return candidate
        index += 1


def fake_get_existing_id_files(path: str):
    """读取测试目录中的编号空文件。"""
    id_files = {"imdb": [], "tmdb": [], "douban": []}
    ext_map = {".imdb": "imdb", ".tmdb": "tmdb", ".douban": "douban"}
    try:
        file_names = os.listdir(path)
    except FileNotFoundError:
        return {"imdb": None, "tmdb": None, "douban": None}, f"目录不存在 {path}"

    for file_name in file_names:
        name, ext = os.path.splitext(file_name)
        key = ext_map.get(ext)
        if key:
            id_files[key].append(name)
    for key, values in id_files.items():
        if len(values) > 1:
            return {"imdb": None, "tmdb": None, "douban": None}, f"目录 {path} 中 {key.upper()} 编号文件太多，请先清理。"
    return {key: values[0] if values else None for key, values in id_files.items()}, None


def fake_remove_duplicates_ignore_case(items: list) -> list:
    """测试环境里的忽略大小写去重。"""
    seen = set()
    result = []
    for item in items:
        key = item.casefold() if isinstance(item, str) else repr(item)
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def load_sort_movie_auto():
    """在隔离依赖的环境中加载 ``sort_movie_auto`` 模块。"""
    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_file_to_list = lambda _path: []
    fake_my_module.read_json_to_dict = lambda _path: {}
    fake_my_module.sanitize_filename = fake_sanitize_filename
    fake_my_module.write_dict_to_json = fake_write_dict_to_json

    fake_sort_movie_mysql = types.ModuleType("sort_movie_mysql")
    fake_sort_movie_mysql.insert_movie_record_to_mysql = lambda _path: None
    fake_sort_movie_mysql.query_imdb_title_metadata = lambda _movie_id: None

    fake_sort_movie_ops = types.ModuleType("sort_movie_ops")
    fake_sort_movie_ops.CONFIG = {
        "video_extensions": [".mkv", ".mp4"],
        "mirror_path": str(Path(tempfile.gettempdir()) / "sort_movie_auto_mirror"),
        "magnet_path": "magnet:?xt=urn:btih:",
    }
    fake_sort_movie_ops.build_unique_path = fake_build_unique_path
    fake_sort_movie_ops.check_local_torrent = lambda _imdb: {"move_counts": 0, "move_files": []}
    fake_sort_movie_ops.delete_trash_files = lambda _path: None
    fake_sort_movie_ops.extract_imdb_id = fake_extract_imdb_id
    fake_sort_movie_ops.get_existing_id_files = fake_get_existing_id_files
    fake_sort_movie_ops.scan_ids = lambda _directory: {"tmdb": None, "douban": None, "imdb": None}
    fake_sort_movie_ops.select_best_yts_magnet = lambda _json_data, magnet_path: f"{magnet_path}{'a' * 40}"
    fake_sort_movie_ops.remove_duplicates_ignore_case = fake_remove_duplicates_ignore_case
    fake_sort_movie_ops.remove_id_marker = lambda path, id_value, suffix: Path(path, f"{id_value}.{suffix}").unlink(missing_ok=True)
    fake_sort_movie_ops.touch_id_marker = lambda path, id_value, suffix: Path(path, f"{id_value}.{suffix}").touch()
    fake_sort_movie_ops.build_movie_folder_name = lambda _path, _movie_dict: "Renamed Movie"
    fake_sort_movie_ops.merged_dict = lambda _path, _movie_info, movie_ids, file_info: movie_ids | file_info
    fake_sort_movie_ops.create_aka_movie = lambda _new_path, _movie_dict: None
    fake_sort_movie_ops.check_movie = lambda _path: None
    fake_sort_movie_ops.get_movie_id = lambda movie_dict: movie_dict.get("imdb")
    fake_sort_movie_ops.fix_douban_name = lambda text: text.strip()

    fake_video_tools = types.ModuleType("video_tools")
    fake_video_tools.VIDEO_EXTENSIONS = [".mkv", ".mp4"]
    fake_video_tools.generate_video_contact = lambda _video_path: None
    fake_video_tools.generate_video_contact_mtn = lambda _video_path: None
    fake_video_tools.get_video_info = lambda _path: None

    fake_sort_movie_request = types.ModuleType("sort_movie_request")
    fake_sort_movie_request.get_tmdb_search_response = lambda _search_id: {}
    fake_sort_movie_request.get_douban_response = lambda _query, _mode: None
    fake_sort_movie_request.get_douban_search_details = lambda _response: None
    fake_sort_movie_request.check_kpk_for_better_quality = lambda _imdb, _quality: None
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
            "video_tools": fake_video_tools,
        },
    ):
        spec.loader.exec_module(module)

    return module


class TestSortMovieAutoKnownRegressions(unittest.TestCase):
    """锁定 ``sort_movie_auto`` 当前关键失败隔离规则。"""

    def setUp(self):
        self.module = load_sort_movie_auto()
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_sort_movie_auto_moves_flattened_folder_when_follow_up_step_fails(self):
        """下游步骤失败时，当前已打平目录应整体移到检验目录。"""
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
            "FAILED_MOVIE_ROOT",
            str(Path(self.temp_dir.name) / "quarantine"),
        ), patch.object(
            self.module,
            "sort_movie",
            side_effect=RuntimeError("metadata lookup failed"),
        ):
            self.module.sort_movie_auto(str(director_dir))

        failed_dir = Path(self.temp_dir.name) / "quarantine" / "Director Name" / movie_dir.name
        self.assertFalse(movie_dir.exists())
        self.assertFalse(nested_file.exists())
        self.assertTrue((failed_dir / "movie.mkv").exists())
        self.assertFalse((failed_dir / "disc1").exists())

    def test_get_douban_movie_info_exits_process_on_unexpected_html(self):
        """豆瓣页面结构异常时，应直接退出进程等待人工检查。"""
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
            with self.assertRaisesRegex(SystemExit, "豆瓣页面解析失败"):
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
        movie_dict = movie_ids | file_info | {"poster_path": "/poster.jpg", "director": "Director Name", "directors": ["Director Name"]}

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
        movie_dict = movie_ids | file_info | {"poster_path": "/poster.jpg", "director": "Director Name", "directors": ["Director Name"]}

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
        movie_dict = movie_ids | file_info | {"poster_path": "/poster.jpg", "director": "Director Name", "directors": ["Director Name"]}

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
            return_value=(True, str(second_movie)),
        ) as mock_sort_movie:
            self.module.sort_movie_auto(str(director_dir))

        self.assertEqual(handled_paths, [str(first_movie), str(second_movie)])
        mock_sort_movie.assert_called_once_with(str(second_movie))
        self.assertFalse(first_movie.exists())
        self.assertTrue((quarantine_root / "Director Name" / first_movie.name).exists())

    def test_sort_movie_auto_moves_folder_when_sort_movie_fails(self):
        """整理阶段失败时，也应将当前电影目录移到检验目录并继续后续目录。"""
        director_dir = Path(self.temp_dir.name) / "Director Name"
        director_dir.mkdir()
        first_movie = director_dir / "01 Bad Movie [tt0000001]"
        second_movie = director_dir / "02 Good Movie [tt0000002]"
        first_movie.mkdir()
        second_movie.mkdir()
        quarantine_root = Path(self.temp_dir.name) / "quarantine"

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
            return_value=None,
        ), patch.object(
            self.module,
            "move_all_files_to_root",
        ), patch.object(
            self.module,
            "sort_movie",
            side_effect=[(False, str(first_movie)), (True, str(second_movie))],
        ) as mock_sort_movie:
            self.module.sort_movie_auto(str(director_dir))

        self.assertEqual(mock_sort_movie.call_count, 2)
        self.assertFalse(first_movie.exists())
        self.assertTrue((quarantine_root / "Director Name" / first_movie.name).exists())
        self.assertTrue(second_movie.exists())

    def test_sort_movie_auto_rolls_back_and_moves_folder_when_validation_fails(self):
        """校验失败时，应先回滚整理产物，再将电影目录移到检验目录。"""
        director_dir = Path(self.temp_dir.name) / "Director Name"
        movie_dir = director_dir / "Movie Folder"
        movie_dir.mkdir(parents=True)
        quarantine_root = Path(self.temp_dir.name) / "quarantine"
        movie_ids = {"tmdb": None, "douban": None, "imdb": "tt1234567"}
        file_info = {
            "source": "BluRay",
            "resolution": "1080p",
            "codec": "h264",
            "bitrate": "8000kbps",
            "duration": 120,
            "quality": "1080p",
        }
        movie_dict = movie_ids | file_info | {"poster_path": "/poster.jpg", "director": "Director Name", "directors": ["Director Name"]}

        def fake_download_cover(_poster_path: str, image_path: str) -> None:
            Path(image_path).write_text("poster", encoding="utf-8")

        def fake_create_aka_movie(new_path: str, _movie_dict: dict) -> None:
            Path(new_path, "Alias Title.别名").write_text("", encoding="utf-8")

        with patch.object(
            self.module,
            "FAILED_MOVIE_ROOT",
            str(quarantine_root),
        ), patch.object(
            self.module,
            "prepare_movie_folder_markers",
            return_value=None,
        ), patch.object(
            self.module,
            "move_all_files_to_root",
        ), patch.object(
            self.module,
            "scan_ids",
            return_value=movie_ids,
        ), patch.object(
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
        ):
            self.module.sort_movie_auto(str(director_dir))

        failed_dir = quarantine_root / "Director Name" / movie_dir.name
        self.assertFalse(movie_dir.exists())
        self.assertTrue(failed_dir.exists())
        self.assertFalse((director_dir / "Renamed Movie").exists())
        self.assertFalse((failed_dir / "tt1234567.jpg").exists())
        self.assertFalse((failed_dir / "Alias Title.别名").exists())
        self.assertFalse((failed_dir / "movie_info.json5").exists())

    def test_sort_movie_auto_moves_folder_when_flattening_fails(self):
        """打平目录失败时，应将当前电影目录移到检验目录并继续后续目录。"""
        director_dir = Path(self.temp_dir.name) / "Director Name"
        director_dir.mkdir()
        first_movie = director_dir / "01 Bad Movie [tt0000001]"
        second_movie = director_dir / "02 Good Movie [tt0000002]"
        first_movie.mkdir()
        second_movie.mkdir()
        quarantine_root = Path(self.temp_dir.name) / "quarantine"

        def fake_move_all_files_to_root(path: str) -> None:
            if path == str(first_movie):
                raise RuntimeError("flatten failed")

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
            return_value=None,
        ), patch.object(
            self.module,
            "move_all_files_to_root",
            side_effect=fake_move_all_files_to_root,
        ), patch.object(
            self.module,
            "sort_movie",
            return_value=(True, str(second_movie)),
        ) as mock_sort_movie:
            self.module.sort_movie_auto(str(director_dir))

        mock_sort_movie.assert_called_once_with(str(second_movie))
        self.assertFalse(first_movie.exists())
        self.assertTrue((quarantine_root / "Director Name" / first_movie.name).exists())
        self.assertTrue(second_movie.exists())


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
        movie_dict = movie_ids | file_info | {"poster_path": "/poster.jpg", "director": "Director Name", "directors": ["Director Name"]}

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

    def test_sort_movie_stops_before_rename_when_screenshot_generation_fails(self):
        """截图生成失败时，应在重命名和落盘之前停止。"""
        movie_dir = Path(self.temp_dir.name) / "Movie Folder"
        movie_dir.mkdir()
        (movie_dir / "movie.mkv").write_text("video", encoding="utf-8")

        movie_ids = {"tmdb": None, "douban": None, "imdb": "tt1234567"}
        file_info = {
            "source": "BluRay",
            "resolution": "1080p",
            "codec": "h264",
            "bitrate": "8000kbps",
            "duration": 120,
            "quality": "1080p",
        }

        with patch.object(self.module, "scan_ids", return_value=movie_ids), patch.object(
            self.module,
            "get_imdb_movie_info",
        ), patch.object(
            self.module,
            "get_video_info",
            return_value=file_info,
        ), patch.object(
            self.module,
            "generate_video_contact",
            side_effect=RuntimeError("screenshot failed"),
        ), patch.object(
            self.module,
            "generate_video_contact_mtn",
        ), patch.object(
            self.module,
            "merged_dict",
        ) as mock_merged_dict, patch.object(
            self.module,
            "apply_sort_movie_transaction",
        ) as mock_transaction:
            self.module.sort_movie(str(movie_dir))

        self.assertTrue(movie_dir.exists())
        self.assertFalse((movie_dir / "movie_s.jpg").exists())
        mock_merged_dict.assert_not_called()
        mock_transaction.assert_not_called()

    def test_sort_movie_stops_before_transaction_when_download_records_are_ambiguous(self):
        """下载记录数量异常时，应在重命名和落盘之前停止。"""
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

        with patch.object(self.module, "scan_ids", return_value=movie_ids), patch.object(
            self.module,
            "get_imdb_movie_info",
        ), patch.object(
            self.module,
            "get_video_info",
            return_value=file_info,
        ), patch.object(
            self.module,
            "ensure_movie_screenshots",
            return_value=None,
        ), patch.object(
            self.module,
            "get_dl_link",
            side_effect=self.module.DownloadLinkError("Movie Folder 目录中下载数量大于 1"),
        ), patch.object(
            self.module,
            "apply_sort_movie_transaction",
        ) as mock_transaction:
            self.module.sort_movie(str(movie_dir))

        self.assertTrue(movie_dir.exists())
        mock_transaction.assert_not_called()

    def test_sort_movie_stops_before_transaction_when_original_title_is_missing(self):
        """缺少原始片名时，应直接停止，不再沿用旧目录名继续整理。"""
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
        movie_dict = movie_ids | file_info | {
            "original_title": "",
            "chinese_title": "",
            "titles": [],
            "poster_path": "/poster.jpg",
            "director": "Director Name",
            "directors": ["Director Name"],
        }

        with patch.object(self.module, "scan_ids", return_value=movie_ids), patch.object(
            self.module,
            "get_imdb_movie_info",
        ), patch.object(
            self.module,
            "get_video_info",
            return_value=file_info,
        ), patch.object(
            self.module,
            "ensure_movie_screenshots",
            return_value=None,
        ), patch.object(
            self.module,
            "merged_dict",
            return_value=movie_dict,
        ), patch.object(
            self.module,
            "apply_sort_movie_transaction",
        ) as mock_transaction:
            result = self.module.sort_movie(str(movie_dir))

        self.assertEqual(result, (False, str(movie_dir)))
        self.assertTrue(movie_dir.exists())
        mock_transaction.assert_not_called()

    def test_build_movie_folder_name_requires_movie_id(self):
        """生成规范目录名时必须有三站任一编号，不再允许 ``noid``。"""
        movie_dir = Path(self.temp_dir.name) / "Movie Folder"
        movie_dir.mkdir()
        movie_dict = {
            "imdb": None,
            "tmdb": None,
            "douban": None,
            "original_title": "Movie",
            "chinese_title": "",
            "year": 2024,
            "source": "BluRay",
            "resolution": "1920x1080",
            "codec": "h264",
            "bitrate": "8000kbps",
        }

        result = self.module.build_movie_folder_name(str(movie_dir), movie_dict)

        self.assertIsNone(result)

    def test_ensure_movie_screenshots_warns_for_multiple_videos(self):
        """多视频目录应提前 warning，但不阻塞截图检查。"""
        movie_dir = Path(self.temp_dir.name) / "Movie Folder"
        movie_dir.mkdir()
        (movie_dir / "movie-a.mkv").write_text("video", encoding="utf-8")
        (movie_dir / "movie-b.mp4").write_text("video", encoding="utf-8")
        (movie_dir / "movie-a_s.jpg").write_text("screenshot", encoding="utf-8")
        (movie_dir / "movie-b_s.jpg").write_text("screenshot", encoding="utf-8")

        with self.assertLogs(self.module.logger, level="WARNING") as cm:
            result = self.module.ensure_movie_screenshots(str(movie_dir))

        self.assertIsNone(result)
        self.assertTrue(any("视频数量大于 1" in message for message in cm.output))

    def test_check_movie_returns_error_for_missing_required_movie_info_field(self):
        """必要字段缺失时，应返回明确错误，而不是让后续流程抛 KeyError。"""
        movie_dir = Path(self.temp_dir.name) / "2024 - Movie{tt1234567}[BluRay][1920x1080][h264@8000kbps]"
        movie_dir.mkdir()
        (movie_dir / "movie_info.json5").write_text("{}", encoding="utf-8")
        movie_info = {
            "director": "",
            "directors": ["Director Name"],
            "imdb": "tt1234567",
            "quality": "1080p",
            "source": "BluRay",
            "duration": 120,
            "runtime_imdb": 120,
            "runtime_tmdb": 120,
        }

        with patch.object(self.module, "read_json_to_dict", return_value=movie_info):
            result = self.module.check_movie(str(movie_dir))

        self.assertEqual(result, f"{movie_dir.name} 缺少必要字段：director")

    def test_check_movie_handles_non_string_directors_and_string_runtimes(self):
        """导演列表和时长字段格式轻微不稳定时，不应让校验流程崩溃。"""
        movie_dir = Path(self.temp_dir.name) / "2024 - Movie{tt1234567}[BluRay][1920x1080][h264@8000kbps]"
        movie_dir.mkdir()
        (movie_dir / "movie_info.json5").write_text("{}", encoding="utf-8")
        movie_info = {
            "director": "Director Name",
            "directors": [None, 123, "Director Name"],
            "imdb": "tt1234567",
            "quality": "1080p",
            "source": "BluRay",
            "duration": "120",
            "runtime_imdb": "120",
            "runtime_tmdb": "123",
        }

        with patch.object(self.module, "read_json_to_dict", return_value=movie_info):
            result = self.module.check_movie(str(movie_dir))

        self.assertIsNone(result)

    def test_check_movie_rejects_noid_directory_name(self):
        """整理完成后的目录名必须包含明确站点编号。"""
        movie_dir = Path(self.temp_dir.name) / "2024 - Movie{noid}[BluRay][1920x1080][h264@8000kbps]"
        movie_dir.mkdir()
        (movie_dir / "movie_info.json5").write_text("{}", encoding="utf-8")
        movie_info = {
            "director": "Director Name",
            "directors": ["Director Name"],
            "imdb": "",
            "tmdb": "",
            "douban": "",
            "quality": "1080p",
            "source": "BluRay",
            "duration": 120,
            "runtime_imdb": 120,
            "runtime_tmdb": 120,
        }

        with patch.object(self.module, "read_json_to_dict", return_value=movie_info):
            result = self.module.check_movie(str(movie_dir))

        self.assertIn("目录名格式错误", result)

    def test_dir_name_regex_accepts_only_supported_movie_ids(self):
        """目录名校验只接受 IMDb、TMDB 和 Douban 的规范编号形态。"""
        valid_names = [
            "1925 - Old Movie{tt0000265}[WEB-DL][640x480][avc@1000kbps]",
            "2024 - Movie{tmdb12}[WEB-DL][1920x1080][avc@8000kbps]",
            "2024 - Series{tmdb12345tv}[WEB-DL][1920x1080][avc@8000kbps]",
            "2024 - Movie{db123456}[WEB-DL][1920x1080][avc@8000kbps]",
        ]
        invalid_names = [
            "2024 - Movie{noid}[WEB-DL][1920x1080][avc@8000kbps]",
            "2024 - Movie{tt123tv}[WEB-DL][1920x1080][avc@8000kbps]",
            "2024 - Movie{tmdb1}[WEB-DL][1920x1080][avc@8000kbps]",
            "2024 - Movie{db123}[WEB-DL][1920x1080][avc@8000kbps]",
        ]

        for name in valid_names:
            with self.subTest(name=name):
                self.assertIsNotNone(self.module.RE_DIR_NAME.match(name))
        for name in invalid_names:
            with self.subTest(name=name):
                self.assertIsNone(self.module.RE_DIR_NAME.match(name))

    def test_maintain_checked_movie_warns_but_does_not_fail_when_local_torrents_are_moved(self):
        """本地库存种子命中时，只应 warning，不应阻塞入库校验。"""
        movie_dir = Path(self.temp_dir.name) / "2024 - Movie{tt1234567}[BluRay][1920x1080][h264@8000kbps]"
        movie_dir.mkdir()
        movie_info = {
            "director": "Director Name",
            "directors": ["Director Name"],
            "imdb": "tt1234567",
            "quality": "1080p",
            "source": "BluRay",
            "duration": 120,
            "runtime_imdb": 120,
            "runtime_tmdb": 120,
        }

        with patch.object(self.module, "read_json_to_dict", return_value=movie_info), patch.object(
            self.module,
            "check_local_torrent",
            return_value={"move_counts": 2, "move_files": ["a.torrent", "b.torrent"]},
        ) as mock_check_local, patch.object(
            self.module,
            "delete_trash_files",
        ), self.assertLogs(self.module.logger, level="WARNING") as cm:
            result = self.module.maintain_checked_movie(str(movie_dir), movie_info)

        self.assertIsNone(result)
        mock_check_local.assert_called_once_with("tt1234567")
        self.assertTrue(any("已移动本地库存种子" in message for message in cm.output))

    def test_maintain_checked_movie_warns_but_does_not_fail_when_local_torrent_check_raises(self):
        """本地库存种子检查异常时，只应 warning，不应阻塞入库校验。"""
        movie_dir = Path(self.temp_dir.name) / "2024 - Movie{tt1234567}[BluRay][1920x1080][h264@8000kbps]"
        movie_dir.mkdir()
        movie_info = {
            "director": "Director Name",
            "directors": ["Director Name"],
            "imdb": "tt1234567",
            "quality": "1080p",
            "source": "BluRay",
            "duration": 120,
            "runtime_imdb": 120,
            "runtime_tmdb": 120,
        }

        with patch.object(self.module, "read_json_to_dict", return_value=movie_info), patch.object(
            self.module,
            "check_local_torrent",
            side_effect=RuntimeError("move failed"),
        ) as mock_check_local, patch.object(
            self.module,
            "delete_trash_files",
        ), self.assertLogs(self.module.logger, level="WARNING") as cm:
            result = self.module.maintain_checked_movie(str(movie_dir), movie_info)

        self.assertIsNone(result)
        mock_check_local.assert_called_once_with("tt1234567")
        self.assertTrue(any("本地库存种子检查失败" in message for message in cm.output))


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
            return_value=(True, str(movie_dir)),
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
            return_value=(True, str(movie_dir)),
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
            return_value=(True, str(movie_dir)),
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


class TestAutoLocalHelpers(unittest.TestCase):
    """验证迁入自动整理模块的本地辅助函数。"""

    def setUp(self):
        self.module = load_sort_movie_auto()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_move_all_files_to_root_moves_nested_files_and_keeps_collisions(self):
        """打平目录时应保留同名文件，子目录清空后删除。"""
        movie_dir = self.root / "Movie Folder"
        nested_dir = movie_dir / "disc1" / "subs"
        nested_dir.mkdir(parents=True)
        root_subtitle = movie_dir / "Traditional.chi.srt"
        nested_subtitle = nested_dir / "Traditional.chi.srt"
        nested_video = movie_dir / "disc1" / "movie.mkv"
        root_subtitle.write_text("root", encoding="utf-8")
        nested_subtitle.write_text("nested", encoding="utf-8")
        nested_video.write_text("video", encoding="utf-8")

        self.module.move_all_files_to_root(str(movie_dir))

        self.assertEqual(root_subtitle.read_text(encoding="utf-8"), "root")
        self.assertEqual((movie_dir / "Traditional.chi(1).srt").read_text(encoding="utf-8"), "nested")
        self.assertEqual((movie_dir / "movie.mkv").read_text(encoding="utf-8"), "video")
        self.assertFalse((movie_dir / "disc1").exists())

    def test_move_all_files_to_root_drops_duplicate_empty_marker_files(self):
        """重复打平空文件时不应生成 ``(1)`` 编号或别名文件。"""
        movie_dir = self.root / "Movie Folder"
        nested_dir = movie_dir / "disc1"
        nested_dir.mkdir(parents=True)
        marker_names = ["tt1234567.imdb", "12345.tmdb", "654321.douban", "Alias Title.别名"]

        for marker_name in marker_names:
            (movie_dir / marker_name).touch()
            (nested_dir / marker_name).touch()

        self.module.move_all_files_to_root(str(movie_dir))

        for marker_name in marker_names:
            self.assertTrue((movie_dir / marker_name).exists())
            self.assertFalse((movie_dir / marker_name.replace(".", "(1).", 1)).exists())
        self.assertFalse(nested_dir.exists())

    def test_remove_duplicates_ignore_case_keeps_first_string_variant(self):
        """标题去重应忽略字符串大小写，并保留第一次出现的写法。"""
        result = self.module.remove_duplicates_ignore_case(["Movie", "movie", "MOVIE", "Other"])

        self.assertEqual(result, ["Movie", "Other"])


class TestDownloadLinkExtraction(unittest.TestCase):
    """验证自动整理流程内下载记录提取的准入和归一化规则。"""

    def setUp(self):
        self.module = load_sort_movie_auto()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_dl_link_rejects_multiple_download_records_before_side_effects(self):
        """多个下载记录应直接报错，且不改写或删除任何记录文件。"""
        movie_dir = self.root / "Movie Folder"
        movie_dir.mkdir()
        log_file = movie_dir / "source.log"
        json_file = movie_dir / "source.json"
        log_file.write_text("original log", encoding="utf-8")
        json_file.write_text('{"original": true}', encoding="utf-8")

        with self.assertRaisesRegex(self.module.DownloadLinkError, "下载数量大于 1"):
            self.module.get_dl_link(str(movie_dir))

        self.assertEqual(log_file.read_text(encoding="utf-8"), "original log")
        self.assertEqual(json_file.read_text(encoding="utf-8"), '{"original": true}')

    def test_get_dl_link_rejects_invalid_single_log_before_rewrite(self):
        """单个 LOG 下载链接无效时，应直接报错且不改写原文件。"""
        movie_dir = self.root / "Movie Folder"
        movie_dir.mkdir()
        log_file = movie_dir / "source.log"
        log_file.write_text("not a magnet", encoding="utf-8")

        with patch.object(self.module, "read_file_to_list", return_value=["not a magnet"]):
            with self.assertRaisesRegex(self.module.DownloadLinkError, "下载链接错误"):
                self.module.get_dl_link(str(movie_dir))

        self.assertEqual(log_file.read_text(encoding="utf-8"), "not a magnet")

    def test_get_dl_link_converts_single_json_record_to_log(self):
        """单个 YTS JSON 记录应提取最佳 magnet，落成同名 LOG 后删除 JSON。"""
        movie_dir = self.root / "Movie Folder"
        movie_dir.mkdir()
        json_file = movie_dir / "source.json"
        json_file.write_text('{"data": {"movie": {"torrents": []}}}', encoding="utf-8")
        magnet = f"magnet:?xt=urn:btih:{'b' * 40}"

        with patch.object(self.module, "read_json_to_dict", return_value={"data": {"movie": {"torrents": []}}}), patch.object(
            self.module,
            "select_best_yts_magnet",
            return_value=magnet,
        ):
            result = self.module.get_dl_link(str(movie_dir))

        self.assertEqual(result, magnet)
        self.assertFalse(json_file.exists())
        self.assertEqual((movie_dir / "source.log").read_text(encoding="utf-8"), magnet)


if __name__ == "__main__":
    unittest.main()
