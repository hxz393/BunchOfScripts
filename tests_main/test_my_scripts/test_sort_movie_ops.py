"""
针对 ``my_scripts.sort_movie_ops`` 中仍作为公共底层能力的函数做定向测试。
"""

import importlib.util
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "sort_movie_ops.py"


def load_sort_movie_ops(check_target: str):
    """隔离外部依赖后加载 ``sort_movie_ops``。"""
    fake_pil = types.ModuleType("PIL")
    fake_pil.Image = types.SimpleNamespace(open=lambda *_args, **_kwargs: None)
    fake_moviepy = types.ModuleType("moviepy")
    fake_moviepy.VideoFileClip = object

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: {
        "trash_list": [],
        "source_list": [],
        "video_extensions": [".mkv", ".mp4"],
        "magnet_path": "magnet:",
        "rarbg_source": "",
        "ttg_source": "",
        "dhd_source": "",
        "sk_source": "",
        "rare_source": "",
        "rls_source": "",
        "check_target": check_target,
        "everything_path": "",
        "ffprobe_path": "",
        "mtm_path": "",
        "mediainfo_path": "",
        "mirror_path": "",
        "ru_path": "",
        "yts_path": "",
        "dhd_path": "",
        "ttg_path": "",
        "sk_path": "",
        "rare_path": "",
    }
    fake_my_module.sanitize_filename = lambda text: text
    fake_my_module.read_file_to_list = lambda _path: []
    fake_my_module.get_file_paths = lambda _path: []
    fake_my_module.remove_target = lambda _path: None
    fake_my_module.get_folder_paths = lambda _path: []

    fake_sort_movie_request = types.ModuleType("sort_movie_request")
    fake_sort_movie_request.check_kpk_for_better_quality = lambda _imdb, _quality: False

    spec = importlib.util.spec_from_file_location(
        f"sort_movie_ops_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "PIL": fake_pil,
            "moviepy": fake_moviepy,
            "my_module": fake_my_module,
            "sort_movie_request": fake_sort_movie_request,
        },
    ):
        spec.loader.exec_module(module)
    return module


class TestSharedHelpers(unittest.TestCase):
    """验证跨电影/导演流程复用的小工具。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.module = load_sort_movie_ops(str(self.root / "check"))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_build_unique_path_appends_counter_before_suffix(self):
        """目标文件已存在时，应生成 ``name(1).ext`` 形式的新路径。"""
        target = self.root / "Movie.torrent"
        target.write_text("first", encoding="utf-8")
        (self.root / "Movie(1).torrent").write_text("second", encoding="utf-8")

        result = self.module.build_unique_path(target)

        self.assertEqual(result, self.root / "Movie(2).torrent")

    def test_id_marker_helpers_round_trip_and_report_duplicates(self):
        """编号空文件读写应和 ``scan_ids`` 使用同一套规则。"""
        movie_dir = self.root / "movie"
        movie_dir.mkdir()

        self.module.touch_id_marker(str(movie_dir), "tt1234567", "imdb")
        self.module.touch_id_marker(str(movie_dir), "12345tv", "tmdb")
        ids, error = self.module.get_existing_id_files(str(movie_dir))

        self.assertIsNone(error)
        self.assertEqual(ids, {"imdb": "tt1234567", "tmdb": "12345tv", "douban": None})
        self.assertEqual(self.module.scan_ids(str(movie_dir)), {"imdb": "tt1234567", "tmdb": "12345tv", "douban": None})

        self.module.remove_id_marker(str(movie_dir), "tt1234567", "imdb")
        self.assertFalse((movie_dir / "tt1234567.imdb").exists())

        (movie_dir / "111.tmdb").touch()
        _ids, duplicate_error = self.module.get_existing_id_files(str(movie_dir))
        self.assertIn("TMDB 编号文件太多", duplicate_error)

    def test_remove_duplicates_ignore_case_handles_non_string_values(self):
        """去重工具应保留首次出现项，并兼容非字符串和不可 hash 值。"""
        items = ["Movie", "movie", 1, 1, ["a"], ["a"], {"x": 1}, {"x": 1}]

        result = self.module.remove_duplicates_ignore_case(items)

        self.assertEqual(result, ["Movie", 1, ["a"], {"x": 1}])


class TestCheckLocalTorrent(unittest.TestCase):
    """验证本地库存种子命中后的移动策略。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.check_target = self.root / "check"
        self.module = load_sort_movie_ops(str(self.check_target))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_check_local_torrent_moves_all_matches_without_deleting(self):
        """命中 IMDb 编号的库存种子都应移动到检查目录，且目标重名时保留两份。"""
        source_dir = self.root / "source"
        source_dir.mkdir()
        self.check_target.mkdir()
        first = source_dir / "Movie [tt1234567].torrent"
        second = source_dir / "Movie [tt1234567] 2160p.torrent"
        other = source_dir / "Other [tt7654321].torrent"
        existing = self.check_target / first.name
        first.write_text("first", encoding="utf-8")
        second.write_text("second", encoding="utf-8")
        other.write_text("other", encoding="utf-8")
        existing.write_text("existing", encoding="utf-8")
        self.module.PRE_LOAD_FP = [str(first), str(second), str(other)]

        result = self.module.check_local_torrent("tt1234567")

        self.assertEqual(result["move_counts"], 2)
        self.assertEqual(result["move_files"], [str(self.check_target / "Movie [tt1234567](1).torrent"), str(self.check_target / second.name)])
        self.assertFalse(first.exists())
        self.assertFalse(second.exists())
        self.assertTrue(other.exists())
        self.assertEqual(existing.read_text(encoding="utf-8"), "existing")
        self.assertEqual((self.check_target / "Movie [tt1234567](1).torrent").read_text(encoding="utf-8"), "first")
        self.assertEqual((self.check_target / second.name).read_text(encoding="utf-8"), "second")


if __name__ == "__main__":
    unittest.main()
