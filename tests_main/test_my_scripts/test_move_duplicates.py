"""
针对 ``my_scripts.move_duplicates`` 的定向单元测试。

这些测试直接使用临时目录验证真实文件移动行为，覆盖：
1. 参数路径校验。
2. 合作艺人目录的识别与移动。
3. 目标目录已存在同名条目时跳过。
4. 没有命中重复候选时返回 ``None``。
"""

import importlib.util
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "move_duplicates.py"


def load_move_duplicates():
    """直接从文件路径加载 ``move_duplicates`` 模块。"""
    spec = importlib.util.spec_from_file_location(
        f"move_duplicates_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestMoveDuplicates(unittest.TestCase):
    def setUp(self):
        self.module = load_move_duplicates()

    def test_returns_none_when_source_path_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            target_dir = Path(temp_dir) / "target"
            target_dir.mkdir()

            with patch.object(self.module.logger, "error") as mock_error:
                result = self.module.move_duplicates(Path(temp_dir) / "missing", target_dir)

        self.assertIsNone(result)
        mock_error.assert_called_once_with(f"源目录不存在：{Path(temp_dir) / 'missing'}")

    def test_moves_collaboration_artist_folder_to_target(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_dir = root / "source"
            target_dir = root / "target"
            source_dir.mkdir()
            target_dir.mkdir()

            (source_dir / "artist a").mkdir()
            duplicate_dir = source_dir / "artist a feat. artist b"
            duplicate_dir.mkdir()
            (duplicate_dir / "note.txt").write_text("marker", encoding="utf-8")

            moved = self.module.move_duplicates(source_dir, target_dir)

            old_path = os.path.normpath(str(duplicate_dir))
            new_path = os.path.normpath(str(target_dir / "artist a"))
            self.assertEqual(moved, {old_path: new_path})
            self.assertFalse(duplicate_dir.exists())
            self.assertTrue((target_dir / "artist a").is_dir())
            self.assertEqual((target_dir / "artist a" / "note.txt").read_text(encoding="utf-8"), "marker")

    def test_skips_move_when_target_already_exists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_dir = root / "source"
            target_dir = root / "target"
            source_dir.mkdir()
            target_dir.mkdir()

            (source_dir / "artist a").mkdir()
            duplicate_dir = source_dir / "artist a feat. artist b"
            duplicate_dir.mkdir()
            (target_dir / "artist a").mkdir()

            with patch.object(self.module.logger, "error") as mock_error:
                moved = self.module.move_duplicates(source_dir, target_dir)

            self.assertIsNone(moved)
            self.assertTrue(duplicate_dir.exists())
            mock_error.assert_called_once_with(
                f"'{os.path.normpath(str(duplicate_dir))}' move skipped. The target "
                f"'{os.path.normpath(str(target_dir / 'artist a'))}' is exist"
            )

    def test_returns_none_when_no_duplicate_candidate_found(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_dir = root / "source"
            target_dir = root / "target"
            source_dir.mkdir()
            target_dir.mkdir()

            (source_dir / "artist a").mkdir()
            (source_dir / "artist b").mkdir()

            moved = self.module.move_duplicates(source_dir, target_dir)

            self.assertIsNone(moved)
            self.assertFalse(any(target_dir.iterdir()))


if __name__ == "__main__":
    unittest.main()
