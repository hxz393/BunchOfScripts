import unittest
import os
import shutil
from pathlib import Path

from my_scripts import *


class TestCreateFoldersBatch(unittest.TestCase):
    def setUp(self):
        # 创建一个临时目录和文件用于测试
        self.test_dir = Path("test_dir")
        self.test_dir.mkdir(exist_ok=True)

        self.test_file = Path("test.txt")
        with self.test_file.open("w") as f:
            f.write("folder1\nfolder2\nfolder3")

    def tearDown(self):
        # 测试结束后删除临时目录和文件
        for child in self.test_dir.iterdir():
            if child.is_file():
                child.unlink()
            else:
                shutil.rmtree(child)
        self.test_dir.rmdir()
        self.test_file.unlink()

    def test_create_folders_batch(self):
        # 测试函数 create_folders_batch
        created_folders = create_folders_batch(self.test_dir, self.test_file)
        self.assertEqual(created_folders, ['folder1', 'folder2', 'folder3'])

        for folder in created_folders:
            self.assertTrue((self.test_dir / folder).exists())


if __name__ == "__main__":
    unittest.main()
