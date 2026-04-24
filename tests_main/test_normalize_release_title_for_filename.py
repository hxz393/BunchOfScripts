"""
针对 ``my_module.file_ops.normalize_release_title_for_filename`` 的单元测试。
"""

import importlib.util
import unittest
import uuid
from pathlib import Path

MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "my_module"
    / "file_ops"
    / "normalize_release_title_for_filename.py"
)


def load_module():
    """直接从文件路径加载被测模块。"""
    spec = importlib.util.spec_from_file_location(
        f"normalize_release_title_for_filename_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestNormalizeReleaseTitleForFilename(unittest.TestCase):
    """验证公共标题规范化函数。"""

    def setUp(self):
        self.module = load_module()

    def test_default_normalization_replaces_special_characters_and_placeholder(self):
        """默认规则应规整管道、路径分隔符、空白和 ``{@}``。"""
        result = self.module.normalize_release_title_for_filename("Title | A / B \\ C {@}")

        self.assertEqual(result, "Title，A｜B｜C .")

    def test_truncates_when_title_exceeds_max_length(self):
        """标题超长时应在标题阶段截断。"""
        long_name = "A" * 240

        result = self.module.normalize_release_title_for_filename(long_name, max_length=230)

        self.assertEqual(result, "A" * 230)

    def test_optional_flags_preserve_mt_style_behavior(self):
        """关闭可选替换时，应保留 ``|`` 和 ``{@}`` 原样。"""
        result = self.module.normalize_release_title_for_filename(
            "Title | A / B {@}",
            replace_pipe=False,
            replace_placeholder_dot=False,
        )

        self.assertEqual(result, "Title | A｜B {@}")

    def test_extra_cleanup_patterns_remove_site_specific_noise(self):
        """额外清理规则应用于站点特有杂质。"""
        result = self.module.normalize_release_title_for_filename(
            "Movie = CSFD 80% / Cut",
            extra_cleanup_patterns=(r"\s*=\s*CSFD\s*\d+%",),
        )

        self.assertEqual(result, "Movie｜Cut")
