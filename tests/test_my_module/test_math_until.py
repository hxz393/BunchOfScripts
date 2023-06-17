import unittest
from my_module import *

class TestFormatSize(unittest.TestCase):

    def test_bytes(self):
        self.assertEqual(format_size(150), '150.00 Bytes')

    def test_kb(self):
        self.assertEqual(format_size(1550), '1.51 KB')

    def test_mb(self):
        self.assertEqual(format_size(1049026), '1.00 MB')

    def test_gb_disk(self):
        self.assertEqual(format_size(1073741824, is_disk=True), '1.07 GB')

    def test_negative_input(self):
        with self.assertRaises(ValueError):
            format_size(-150)

    def test_non_numeric_input(self):
        with self.assertRaises(TypeError):
            format_size('150')

class TestFormatTime(unittest.TestCase):

    def test_seconds(self):
        self.assertEqual(format_time(65), '0h 1m 5s')

    def test_hours(self):
        self.assertEqual(format_time(3600), '1h 0m 0s')

    def test_float_input(self):
        self.assertEqual(format_time(3661.67), '1h 1m 1s')

    def test_negative_input(self):
        with self.assertRaises(ValueError):
            format_time(-150)

class TestCalculateTransferSpeed(unittest.TestCase):

    def test_kb_per_second(self):
        self.assertEqual(calculate_transfer_speed(1024, 1), '1.00 KB/s')

    def test_higher_speed(self):
        self.assertEqual(calculate_transfer_speed(1024, 0.5), '2.00 KB/s')

    def test_mb_per_second(self):
        self.assertEqual(calculate_transfer_speed(198548576, 1), '189.35 MB/s')

    def test_gb_per_second(self):
        self.assertEqual(calculate_transfer_speed(11173741824, 19), '560.85 MB/s')

    def test_zero_time(self):
        with self.assertRaises(ValueError):
            calculate_transfer_speed(1024, 0)

    def test_negative_size(self):
        with self.assertRaises(ValueError):
            calculate_transfer_speed(-1024, 1)

    def test_negative_time(self):
        with self.assertRaises(ValueError):
            calculate_transfer_speed(1024, -1)

    def test_non_int_size(self):
        with self.assertRaises(TypeError):
            calculate_transfer_speed(1024.0, 1)

    def test_non_numeric_time(self):
        with self.assertRaises(TypeError):
            calculate_transfer_speed(1024, '1')

if __name__ == '__main__':
    unittest.main()
