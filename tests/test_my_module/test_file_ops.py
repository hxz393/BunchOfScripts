import unittest
import os
import json
from pathlib import Path
import tempfile
import shutil
import time

from my_module import *


class TestGetFilePaths(unittest.TestCase):
    def setUp(self):
        # 创建临时目录和文件用于测试
        self.test_dir = tempfile.mkdtemp()
        self.sub_dir = os.path.join(self.test_dir, "sub_dir")
        os.makedirs(self.sub_dir)
        self.test_files = [
                              os.path.join(self.test_dir, f"test_file{i}.txt") for i in range(3)
                          ] + [os.path.join(self.sub_dir, f"test_file{i}.txt") for i in range(3, 6)]
        for file in self.test_files:
            with open(file, "w") as f:
                f.write("test content")

    def tearDown(self):
        # 测试结束后清理临时文件和目录
        shutil.rmtree(self.test_dir)

    def test_get_file_paths(self):
        file_paths_list = get_file_paths(target_path=self.test_dir)
        # 确保找到了所有文件
        self.assertEqual(set(file_paths_list), set(self.test_files))

    def test_no_directories_in_output(self):
        file_paths_list = get_file_paths(target_path=self.test_dir)

        # 检查返回的路径列表中是否所有的路径都不是目录
        self.assertTrue(all(not os.path.isdir(path) for path in file_paths_list))

    def test_not_exist_path(self):
        file_paths_list = get_file_paths(target_path="not_exist_path")
        self.assertEqual(file_paths_list, None)

    def test_not_dir_path(self):
        not_dir_path = self.test_files[0]
        file_paths_list = get_file_paths(target_path=not_dir_path)
        self.assertEqual(file_paths_list, None)


class TestGetFilePathsByType(unittest.TestCase):
    def setUp(self):
        # 创建临时目录和文件用于测试
        self.test_dir = tempfile.mkdtemp()
        self.sub_dir = os.path.join(self.test_dir, "sub_dir")
        os.makedirs(self.sub_dir)
        self.test_files = {
            os.path.join(self.test_dir, f"test_file{i}.txt"): '.txt' for i in range(3)
        }
        self.test_files.update({
            os.path.join(self.sub_dir, f"test_file{i}.pdf"): '.pdf' for i in range(3, 6)
        })
        for file in self.test_files.keys():
            with open(file, "w") as f:
                f.write("test content")

    def tearDown(self):
        # 测试结束后清理临时文件和目录
        shutil.rmtree(self.test_dir)

    def test_get_file_paths_by_type(self):
        for type_list in [['.txt'], ['.pdf'], ['.txt', '.pdf']]:
            file_paths_list = get_file_paths_by_type(target_path=self.test_dir, type_list=type_list)
            expected_paths = [file for file, file_type in self.test_files.items() if file_type in type_list]
            self.assertEqual(set(file_paths_list), set(expected_paths))


class TestGetFileType(unittest.TestCase):
    def setUp(self):
        # 创建临时目录和文件用于测试
        self.test_dir = tempfile.mkdtemp()

        # 文本文件
        self.test_files = {
            os.path.join(self.test_dir, f"test_file{i}.txt"): 'text/plain' for i in range(3)
        }
        for file in self.test_files.keys():
            with open(file, "w") as f:
                f.write("test content")

        # PDF文件
        self.pdf_file = os.path.join(self.test_dir, "test_file.pdf")
        with open(self.pdf_file, "wb") as f:
            f.write(b"%PDF-1.5 %\n")  # 创建一个简单的PDF文件
        self.test_files[self.pdf_file] = 'application/pdf'

        # JPEG文件
        self.jpg_file = os.path.join(self.test_dir, "test_file.jpg")
        with open(self.jpg_file, "wb") as f:
            f.write(b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00")  # 创建一个简单的JPEG文件
        self.test_files[self.jpg_file] = 'image/jpeg'

    def tearDown(self):
        # 测试结束后清理临时文件和目录
        shutil.rmtree(self.test_dir)

    def test_get_file_type(self):
        for file, expected_type in self.test_files.items():
            result_type = get_file_type(target_path=file)
            self.assertEqual(result_type, expected_type)


class TestGetFolderPaths(unittest.TestCase):
    def setUp(self):
        # 创建临时目录和子目录用于测试
        self.test_dir = tempfile.mkdtemp()
        self.test_subdirs = [os.path.join(self.test_dir, f"subdir{i}") for i in range(3)]
        for subdir in self.test_subdirs:
            os.mkdir(subdir)

    def tearDown(self):
        # 测试结束后清理临时目录
        shutil.rmtree(self.test_dir)

    def test_get_folder_paths(self):
        result_paths = get_folder_paths(target_path=self.test_dir)
        # 比较路径列表，需要先排序以防止顺序问题导致的测试失败
        self.assertCountEqual(sorted(result_paths), sorted(self.test_subdirs))


class TestGetResourcePath(unittest.TestCase):

    def test_valid_relative_path(self):
        with open('temp.txt', 'w') as f:
            f.write('This is a temporary file for testing.')

        absolute_path = get_resource_path('temp.txt')

        self.assertEqual(absolute_path, os.path.abspath('temp.txt'))

        os.remove('temp.txt')

    def test_non_existing_relative_path(self):
        absolute_path = get_resource_path('non_existing.txt')

        self.assertEqual(absolute_path, os.path.abspath('non_existing.txt'))


class TestGetSubdirectories(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.sub_dirs = ['dir1', 'dir2', 'dir3']
        for dir_name in self.sub_dirs:
            os.mkdir(os.path.join(self.test_dir.name, dir_name))
        os.mkdir(os.path.join(self.test_dir.name, self.sub_dirs[0], self.sub_dirs[1]))

    def test_get_subdirectories(self):
        result = get_subdirectories(self.test_dir.name)
        result_dirs = [os.path.basename(dir_path) for dir_path in result]
        self.assertEqual(set(result_dirs), set(self.sub_dirs))

    def tearDown(self):
        self.test_dir.cleanup()


class TestGetTargetSize(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.sub_dirs = ['dir1', 'dir2', 'dir3']
        self.file_sizes = [100, 200, 300]
        for dir_name, file_size in zip(self.sub_dirs, self.file_sizes):
            os.mkdir(os.path.join(self.test_dir.name, dir_name))
            with open(os.path.join(self.test_dir.name, dir_name, 'test_file.txt'), 'w') as f:
                f.write('0' * file_size)

    def test_get_target_size_file(self):
        for dir_name, file_size in zip(self.sub_dirs, self.file_sizes):
            result = get_target_size(os.path.join(self.test_dir.name, dir_name, 'test_file.txt'))
            self.assertEqual(result, file_size)

    def test_get_target_size_dir(self):
        result = get_target_size(self.test_dir.name)
        self.assertEqual(result, sum(self.file_sizes))

    def tearDown(self):
        self.test_dir.cleanup()


class TestMoveFolderWithRename(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.source_path = Path(self.test_dir.name) / 'source_file'
        self.source_path.write_text('This is a test file.')
        self.target_dir = Path(self.test_dir.name)

    def test_move_file_with_rename(self):
        target_path = self.target_dir / self.source_path.name
        result = move_folder_with_rename(self.source_path, target_path)
        self.assertTrue(result.exists())
        self.assertFalse(self.source_path.exists())

    def test_move_file_existing_target(self):
        target_path = self.target_dir / self.source_path.name
        with open(target_path, 'w') as f:
            f.write('dummy text')
        result = move_folder_with_rename(self.source_path, target_path)
        self.assertTrue(result.exists())
        self.assertNotEqual(self.source_path.name, result.name)

    def tearDown(self):
        self.test_dir.cleanup()
        if self.source_path.exists():
            self.source_path.unlink()


class ReadFileToListTest(unittest.TestCase):
    def setUp(self):
        self.test_file = tempfile.NamedTemporaryFile(delete=False)
        self.test_file.write(b"Line 1\nLine 2\nLine 3\n")
        self.test_file.close()

        self.test_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        os.remove(self.test_file.name)
        self.test_dir.cleanup()

    def test_read_file_to_list(self):
        expected_output = ['Line 1', 'Line 2', 'Line 3']
        self.assertListEqual(read_file_to_list(self.test_file.name), expected_output)


class ReadJsonToDictTest(unittest.TestCase):
    def setUp(self):
        self.test_file = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
        json.dump({"key": "value"}, self.test_file)
        self.test_file.close()

        self.test_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        os.remove(self.test_file.name)
        self.test_dir.cleanup()

    def test_read_json_to_dict(self):
        expected_output = {"key": "value"}
        self.assertDictEqual(read_json_to_dict(self.test_file.name), expected_output)


class RemoveEmptyFoldersTest(unittest.TestCase):
    def setUp(self):
        self.test_dir_with_subdirs = tempfile.TemporaryDirectory()

        self.empty_subdir_1 = os.path.join(self.test_dir_with_subdirs.name, "subdir1")
        self.empty_subdir_2 = os.path.join(self.test_dir_with_subdirs.name, "subdir2")

        os.makedirs(self.empty_subdir_1)
        os.makedirs(self.empty_subdir_2)

    def tearDown(self):
        self.test_dir_with_subdirs.cleanup()

    def test_remove_empty_folders(self):
        removed_dirs = remove_empty_folders(self.test_dir_with_subdirs.name)

        self.assertIn(self.empty_subdir_1, removed_dirs)
        self.assertIn(self.empty_subdir_2, removed_dirs)
        self.assertFalse(os.path.exists(self.empty_subdir_1))
        self.assertFalse(os.path.exists(self.empty_subdir_2))

    def test_directory_with_file_is_not_removed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with tempfile.NamedTemporaryFile(dir=temp_dir):
                removed_dirs = remove_empty_folders(temp_dir)

                self.assertTrue(os.path.exists(temp_dir))
                self.assertNotIn(temp_dir, removed_dirs)


class TestRemoveRedundantDirs(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.sub_dir = os.path.join(self.test_dir, "test_dir")
        self.redundant_dir = os.path.join(self.sub_dir, "test_dir")
        os.makedirs(self.redundant_dir)
        self.file_in_redundant_dir = os.path.join(self.redundant_dir, "test_file.txt")
        with open(self.file_in_redundant_dir, "w") as f:
            f.write("test content")

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_remove_redundant_dirs(self):
        removed_dirs = remove_redundant_dirs(target_path=self.test_dir)
        self.assertEqual(removed_dirs, [os.path.normpath(self.redundant_dir)])
        self.assertTrue(os.path.exists(os.path.join(self.sub_dir, "test_file.txt")))

    def test_multi_subdirs_not_redundant(self):
        extra_subdir = os.path.join(self.sub_dir, "extra_dir")
        os.mkdir(extra_subdir)
        removed_dirs = remove_redundant_dirs(target_path=self.test_dir)
        self.assertEqual(removed_dirs, [])
        self.assertTrue(os.path.exists(self.sub_dir))
        self.assertTrue(os.path.exists(extra_subdir))


class TestRemoveTarget(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.test_file = os.path.join(self.test_dir.name, "test_file.txt")
        with open(self.test_file, "w") as f:
            f.write("test content")

    def tearDown(self):
        self.test_dir.cleanup()

    def test_remove_file(self):
        remove_target(self.test_file)
        self.assertFalse(Path(self.test_file).exists(), "The file should be removed.")

    def test_remove_directory(self):
        remove_target(self.test_dir.name)
        self.assertFalse(Path(self.test_dir.name).exists(), "The directory should be removed.")


class TestRemoveTargetMatched(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.match_list = ['test_file1.txt', 'test_file2.txt', 'subdir1']
        self.matched_files = [os.path.join(self.test_dir.name, filename) for filename in self.match_list[:2]]
        self.matched_dir = os.path.join(self.test_dir.name, self.match_list[2])
        os.makedirs(self.matched_dir)
        for file in self.matched_files:
            with open(file, "w") as f:
                f.write("test content")

    def tearDown(self):
        self.test_dir.cleanup()

    def test_remove_matched_targets(self):
        removed_paths = remove_target_matched(self.test_dir.name, self.match_list)
        self.assertEqual(set(removed_paths), set(self.matched_files + [self.matched_dir]))
        for path in removed_paths:
            self.assertFalse(Path(path).exists(), f"The path '{path}' should be removed.")


class TestRenameTargetIfExist(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.test_subdir = os.path.join(self.test_dir.name, "subdir")
        os.mkdir(self.test_subdir)
        self.test_file = os.path.join(self.test_subdir, "test_file.txt")
        with open(self.test_file, "w") as f:
            f.write("test content")

    def tearDown(self):
        self.test_dir.cleanup()

    def test_rename_existing_file(self):
        new_path = rename_target_if_exist(self.test_file)
        self.assertEqual(new_path, Path(f"{self.test_file.rsplit('.', 1)[0]}_(1).txt"))
        self.assertTrue(Path(self.test_file).exists(), "The original file should still exist.")
        self.assertFalse(new_path.exists(), "The new path should not exist.")

    def test_rename_existing_dir(self):
        new_path = rename_target_if_exist(self.test_subdir)
        self.assertEqual(new_path, Path(f"{self.test_subdir}_(1)"))
        self.assertTrue(Path(self.test_subdir).exists(), "The original directory should still exist.")
        self.assertFalse(new_path.exists(), "The new path should not exist.")

    def test_non_existing_path(self):
        non_existing_path = os.path.join(self.test_dir.name, "non_existing_path")
        new_path = rename_target_if_exist(non_existing_path)
        self.assertEqual(new_path, Path(non_existing_path))
        self.assertFalse(Path(non_existing_path).exists(), "The non-existing path should not exist.")
        self.assertEqual(new_path, Path(non_existing_path), "The new path should equal to the non-existing path.")


class TestSanitizeFilename(unittest.TestCase):

    def test_with_forbidden_chars(self):
        filename = r'filename?with/special*chars:'
        sanitized = sanitize_filename(filename)
        expected = r'filename-with-special-chars-'
        self.assertEqual(sanitized, expected, "The sanitized filename did not match the expected result.")

    def test_without_forbidden_chars(self):
        filename = r'regular_filename'
        sanitized = sanitize_filename(filename)
        self.assertEqual(sanitized, filename, "The sanitized filename should be the same as the input when there are no forbidden characters.")

    def test_empty_string(self):
        filename = ''
        sanitized = sanitize_filename(filename)
        self.assertEqual(sanitized, filename, "The sanitized filename should be the same as the input when the input is an empty string.")


class TestWriteDictToJson(unittest.TestCase):

    def setUp(self):
        self.data = {
            "name": "中文",
            "age": 30,
            "address": ["New York", 119],
            "pets": {"cat": "meow", "tiger": None}
        }
        self.target_path = Path(__file__).parent / 'resources/test.json'

    def tearDown(self):
        if self.target_path.exists():
            self.target_path.unlink()

    def test_write_to_json(self):
        result = write_dict_to_json(self.target_path, self.data)
        self.assertTrue(result, "The function should return True if successful.")
        with self.target_path.open(encoding='utf-8') as f:
            data = json.load(f)
            self.assertEqual(data, self.data, "The data written to the file does not match the input data.")


class TestWriteListToFile(unittest.TestCase):

    def setUp(self):
        self.content = [1, 'a', '啊']
        self.target_path = Path(__file__).parent / 'resources/test.txt'

    def tearDown(self):
        if self.target_path.exists():
            self.target_path.unlink()

    def test_write_to_file(self):
        result = write_list_to_file(self.target_path, self.content)
        self.assertTrue(result, "The function should return True if successful.")
        with self.target_path.open(encoding='utf-8') as f:
            data = f.read().split('\n')
            self.assertEqual(data, [str(x) for x in self.content],
                             "The data written to the file does not match the input data.")


if __name__ == "__main__":
    unittest.main()
