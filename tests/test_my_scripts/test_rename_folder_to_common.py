import unittest
import os
import tempfile
import shutil
from pathlib import Path

from my_scripts import *
from my_scripts.rename_folder_to_common import MODIFY_RULES, EXCLUDE_CHARS


class TestRenameFolderToCommon(unittest.TestCase):
    def setUp(self):
        self.source_path = tempfile.mkdtemp()
        self.target_path = tempfile.mkdtemp()
        os.makedirs(self.source_path, exist_ok=True)
        os.makedirs(self.target_path, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.source_path)
        shutil.rmtree(self.target_path)

    def test_source_not_exists(self):
        with self.assertRaises(ValueError):
            rename_folder_to_common('not_existing_folder', self.target_path)

    def test_target_not_exists(self):
        with self.assertRaises(ValueError):
            rename_folder_to_common(self.source_path, 'not_existing_folder')

    def test_source_not_directory(self):
        with open(f"{self.source_path}/test_file", 'w') as file:
            file.write("Hello, world!")
        with self.assertRaises(ValueError):
            rename_folder_to_common(f"{self.source_path}/test_file", self.target_path)

    def test_target_not_directory(self):
        with open(f"{self.target_path}/test_file", 'w') as file:
            file.write("Hello, world!")
        with self.assertRaises(ValueError):
            rename_folder_to_common(self.source_path, f"{self.target_path}/test_file")

    def test_target_folder_exists(self):
        os.makedirs(f"{self.source_path}/test∗folder", exist_ok=True)
        os.makedirs(f"{self.target_path}/test-folder", exist_ok=True)
        with self.assertRaises(Exception):
            rename_folder_to_common(self.source_path, self.target_path)

    def test_normal_rename_and_move(self):
        original_folder_name = "thé tèst fõlder"
        expected_folder_name = "the test folder"
        os.makedirs(f"{self.source_path}/{original_folder_name}", exist_ok=True)
        result = rename_folder_to_common(self.source_path, self.target_path)
        self.assertEqual(result, {f"{self.source_path}\\{original_folder_name}": f"{self.target_path}\\{expected_folder_name}"})
        self.assertTrue(Path(f"{self.target_path}/{expected_folder_name}").exists())
        self.assertFalse(Path(f"{self.source_path}/{original_folder_name}").exists())

    def test_exclude_chars(self):
        for char in EXCLUDE_CHARS:
            original_folder_name = "test" + char + "folder"
            expected_folder_name = "test" + char + "folder"

            os.makedirs(f"{self.source_path}/{original_folder_name}", exist_ok=True)
            result = rename_folder_to_common(self.source_path, self.target_path)
            self.assertEqual(result, {})
            self.assertFalse(Path(f"{self.target_path}/{expected_folder_name}").exists())
            self.assertTrue(Path(f"{self.source_path}/{original_folder_name}").exists())

            # 清除已创建的文件夹以进行下一次迭代
            shutil.rmtree(f"{self.source_path}/{original_folder_name}")

    def test_modify_rules(self):
        test_cases = [
            ("the foldername", "foldername"),  # 测试规则 (r'^the\s', ' ')
            ("foldername  name", "foldername name"),  # 测试规则 (r'\s\s', ' ')
            ("foldername, the", "foldername"),  # 测试规则 (r', the$', ' ')
            ("foldername`name", "foldername-name"),  # 测试规则 (r'`', '-')
            ("foldername∶name", "foldername-name"),  # 测试规则 (r'∶', '-')
            ("foldername∗name", "foldername-name"),  # 测试规则 (r'∗', '-')
            ("foldername？name", "foldernamename")  # 测试规则 (r'？', '')
        ]

        for original_folder_name, expected_folder_name in test_cases:
            os.makedirs(f"{self.source_path}/{original_folder_name}", exist_ok=True)
            result = rename_folder_to_common(self.source_path, self.target_path)
            self.assertEqual(result, {f"{self.source_path}\\{original_folder_name}": f"{self.target_path}\\{expected_folder_name}"})
            self.assertTrue(Path(f"{self.target_path}/{expected_folder_name}").exists())
            self.assertFalse(Path(f"{self.source_path}/{original_folder_name}").exists())

            # 清除已创建的文件夹以进行下一次迭代
            shutil.rmtree(f"{self.target_path}/{expected_folder_name}")


if __name__ == "__main__":
    unittest.main()