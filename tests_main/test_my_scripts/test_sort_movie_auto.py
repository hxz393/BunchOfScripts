"""
针对 ``my_scripts.sort_movie_auto`` 的定向单元测试。

这些用例先把当前已知的高风险问题固化下来：
1. 下游步骤失败时，不应提前打平目录结构。
2. 豆瓣页面解析异常时，不应直接退出整个进程。
3. 校验失败时，不应先重命名目录或落盘半成品。
4. 批处理中的单个目录失败，不应中断后续目录。

当前生产代码尚未修复，因此这些用例先标记为 ``expectedFailure``。
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


def fake_read_file_to_list(target_path: str | os.PathLike) -> list[str] | None:
    """按项目当前习惯读取 UTF-8 文本并返回非空行。"""
    path = Path(target_path)
    if not path.exists() or not path.is_file():
        return None
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def fake_write_list_to_file(target_path: str | os.PathLike, content: list[object]) -> bool:
    """将列表逐行写入 UTF-8 文本。"""
    path = Path(target_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(str(item) for item in content), encoding="utf-8")
    return True


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
    fake_my_module.read_file_to_list = fake_read_file_to_list
    fake_my_module.write_list_to_file = fake_write_list_to_file
    fake_my_module.sanitize_filename = fake_sanitize_filename
    fake_my_module.write_dict_to_json = fake_write_dict_to_json

    fake_sort_movie_mysql = types.ModuleType("sort_movie_mysql")
    fake_sort_movie_mysql.insert_movie_record_to_mysql = lambda _path: None
    fake_sort_movie_mysql.query_imdb_title_metadata = lambda _movie_id: None

    fake_sort_movie_ops = types.ModuleType("sort_movie_ops")
    fake_sort_movie_ops.get_ids = lambda _source_path: None
    fake_sort_movie_ops.move_all_files_to_root = fake_move_all_files_to_root
    fake_sort_movie_ops.extract_imdb_id = fake_extract_imdb_id
    fake_sort_movie_ops.scan_ids = lambda _directory: {"tmdb": None, "douban": None, "imdb": None}
    fake_sort_movie_ops.safe_get = fake_safe_get
    fake_sort_movie_ops.build_movie_folder_name = lambda _path, _movie_dict: "Renamed Movie"
    fake_sort_movie_ops.merged_dict = lambda _path, _movie_info, movie_ids, file_info: movie_ids | file_info
    fake_sort_movie_ops.create_aka_movie = lambda _new_path, _movie_dict: None
    fake_sort_movie_ops.get_video_info = lambda _path: None
    fake_sort_movie_ops.check_movie = lambda _path: None
    fake_sort_movie_ops.get_movie_id = lambda movie_dict: movie_dict.get("imdb") or "noid"
    fake_sort_movie_ops.fix_douban_name = lambda text: text.strip()

    fake_sort_movie_request = types.ModuleType("sort_movie_request")
    fake_sort_movie_request.IMDB_MOVIE_URL = "https://www.imdb.com/title"
    fake_sort_movie_request.TMDB_URL = "https://www.themoviedb.org/movie"
    fake_sort_movie_request.get_tmdb_search_response = lambda _search_id: {}
    fake_sort_movie_request.get_douban_response = lambda _query, _mode: None
    fake_sort_movie_request.get_douban_search_details = lambda _response: None
    fake_sort_movie_request.get_tmdb_movie_details = lambda _movie_id, _tv=False: {}
    fake_sort_movie_request.get_imdb_movie_details = lambda _movie_id: {}
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
        target_file = Path(self.temp_dir.name) / "movie_links.txt"

        def fake_sort_movie_auto_folder(path: str, temp_file: str) -> None:
            Path(temp_file).write_text(f"{path}\nhttps://www.imdb.com/title/tt1234567/\n", encoding="utf-8")
            return None

        with patch.object(
            self.module,
            "sort_movie_auto_folder",
            side_effect=fake_sort_movie_auto_folder,
        ), patch.object(
            self.module,
            "sort_movie",
            side_effect=RuntimeError("metadata lookup failed"),
        ):
            with self.assertRaises(RuntimeError):
                self.module.sort_movie_auto(str(director_dir), str(target_file))

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

    @unittest.expectedFailure
    def test_sort_movie_waits_for_validation_before_rename_and_write_side_effects(self):
        """校验失败时，不应先改目录名或写半成品文件。"""
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

        renamed_dir = movie_dir.parent / "Renamed Movie"
        self.assertTrue(movie_dir.exists())
        self.assertFalse(renamed_dir.exists())
        mock_cover.assert_not_called()
        mock_aliases.assert_not_called()
        mock_write_json.assert_not_called()
        mock_insert.assert_not_called()

    @unittest.expectedFailure
    def test_sort_movie_auto_continues_after_one_folder_fails(self):
        """单个目录失败后，后续目录仍应继续处理。"""
        director_dir = Path(self.temp_dir.name) / "Director Name"
        director_dir.mkdir()
        first_movie = director_dir / "01 Bad Movie [tt0000001]"
        second_movie = director_dir / "02 Good Movie [tt0000002]"
        first_movie.mkdir()
        second_movie.mkdir()
        target_file = Path(self.temp_dir.name) / "movie_links.txt"
        handled_paths: list[str] = []

        def fake_sort_movie_auto_folder(path: str, temp_file: str) -> str | None:
            handled_paths.append(path)
            if path.endswith("01 Bad Movie [tt0000001]"):
                return "missing imdb"
            Path(temp_file).write_text(f"{path}\nhttps://www.imdb.com/title/tt0000002/\n", encoding="utf-8")
            return None

        with patch.object(
            self.module.os,
            "listdir",
            return_value=[first_movie.name, second_movie.name],
        ), patch.object(
            self.module,
            "sort_movie_auto_folder",
            side_effect=fake_sort_movie_auto_folder,
        ), patch.object(
            self.module,
            "move_all_files_to_root",
        ), patch.object(
            self.module,
            "sort_movie",
        ) as mock_sort_movie:
            self.module.sort_movie_auto(str(director_dir), str(target_file))

        self.assertEqual(handled_paths, [str(first_movie), str(second_movie)])
        mock_sort_movie.assert_called_once_with(str(second_movie))


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


if __name__ == "__main__":
    unittest.main()
