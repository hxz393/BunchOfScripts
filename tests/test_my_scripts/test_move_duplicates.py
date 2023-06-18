import unittest
import os
import shutil
import tempfile

from my_scripts import *


class TestMoveDuplicates(unittest.TestCase):
    def setUp(self):
        self.source_path = tempfile.mkdtemp()
        self.target_path = tempfile.mkdtemp()

        os.mkdir(os.path.join(self.source_path, 'duplicate1'))
        os.mkdir(os.path.join(self.source_path, 'duplicate2 feat. duplicate1'))
        os.mkdir(os.path.join(self.source_path, 'duplicate3 ft duplicate1'))
        os.mkdir(os.path.join(self.source_path, 'duplicate3 feat. duplicate2'))

    def test_move_duplicates(self):
        try:
            result = move_duplicates(self.source_path, self.target_path)
            self.assertEqual(len(result), 1)
            self.assertTrue(os.path.exists(os.path.join(self.target_path, 'duplicate1')))
            self.assertFalse(os.path.exists(os.path.join(self.source_path, 'duplicate2 feat. duplicate1')))
            self.assertTrue(os.path.exists(os.path.join(self.source_path, 'duplicate3 ft duplicate1')))
            self.assertTrue(os.path.exists(os.path.join(self.source_path, 'duplicate3 feat. duplicate2')))
        except Exception as e:
            self.fail(f"Test failed with exception: {str(e)}")

    def tearDown(self):
        shutil.rmtree(self.source_path)
        shutil.rmtree(self.target_path)


if __name__ == '__main__':
    unittest.main()
