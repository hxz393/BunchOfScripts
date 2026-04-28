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
