import unittest
import os
from pathlib import Path

from my_scripts import *


class TestCreateFoldersBatch(unittest.TestCase):
    def setUp(self):
        self.base_dir = Path(__file__).parent / 'resources'
        self.target_path = self.base_dir / 'valid_path'
        self.target_path.mkdir(exist_ok=True)
        self.txt_file = self.base_dir / 'valid_file.txt'
        with open(self.txt_file, "w") as file:
            file.write("folder1\nfolder2\nfolder3")

    def test_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            create_folders_batch('invalid_path', 'invalid_file.txt')

    def test_empty_list(self):
        empty_file = self.base_dir / 'empty_file.txt'
        with open(empty_file, "w") as file:
            file.write("")
        with self.assertRaises(ValueError):
            create_folders_batch(self.target_path, empty_file)

    def test_path_length_exceeded(self):
        long_folder_name_file = self.base_dir / 'long_folder_name_file.txt'
        with open(long_folder_name_file, "w") as file:
            file.write('a' * 261)
        with self.assertRaises(ValueError):
            create_folders_batch(self.target_path, long_folder_name_file)

    def tearDown(self):
        for child in self.base_dir.iterdir():
            if child.is_file():
                child.unlink()
            else:
                for grandchild in child.iterdir():
                    grandchild.unlink()
                child.rmdir()


if __name__ == '__main__':
    unittest.main()
