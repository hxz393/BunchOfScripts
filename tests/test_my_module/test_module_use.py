import unittest
import logging
import os
import configparser
from my_module import *


class TestLangconvChsToCht(unittest.TestCase):

    def test_convert_simplified_to_traditional(self):
        simplified_string = '转换简体到繁体'
        expected_result = '轉換簡體到繁體'

        try:
            result = langconv_chs_to_cht(simplified_string)
            self.assertEqual(result, expected_result)
        except Exception as e:
            self.fail(f"Test failed due to {str(e)}")

    def test_empty_string_input(self):
        with self.assertRaises(ValueError):
            langconv_chs_to_cht('')

    def test_non_string_input(self):
        with self.assertRaises(TypeError):
            langconv_chs_to_cht(123)


class TestLangconvChtToChs(unittest.TestCase):

    def test_convert_traditional_to_simplified(self):
        traditional_string = '轉換繁體到簡體'
        expected_result = '转换繁体到简体'

        try:
            result = langconv_cht_to_chs(traditional_string)
            self.assertEqual(result, expected_result)
        except Exception as e:
            self.fail(f"Test failed due to {str(e)}")

    def test_empty_string_input(self):
        with self.assertRaises(ValueError):
            langconv_cht_to_chs('')

    def test_non_string_input(self):
        with self.assertRaises(TypeError):
            langconv_cht_to_chs(123)


class TestLoggingConfig(unittest.TestCase):

    def test_invalid_log_level(self):
        with self.assertRaises(ValueError):
            logging_config(log_level="INVALID")

    def test_logger_initialization(self):
        logger = logging_config(console_output=True, log_level='INFO')
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.level, logging.INFO)
        self.assertTrue(logger.handlers)


class TestConfigWrite(unittest.TestCase):
    def setUp(self):
        self.target_path = r"resources/test_module_use/test_config.ini"
        self.config = {
            "DEFAULT": {
                "key3": "value3",
                "keyc": 0,
            },
            "section1": {
                "key1": "value1",
                "key2": 1,
            },
            "section2": {
                "keyA": 2.44,
                "keyB": True,
                "keyC": None,
            },
        }

    def tearDown(self):
        if os.path.exists(self.target_path):
            os.remove(self.target_path)

    def test_empty_path(self):
        with self.assertRaises(ValueError):
            config_write(target_path="", config=self.config)

    def test_empty_config(self):
        with self.assertRaises(ValueError):
            config_write(target_path=self.target_path, config={})

    def test_config_write(self):
        config_write(target_path=self.target_path, config=self.config)
        self.assertTrue(os.path.exists(self.target_path))
        config_parser = configparser.ConfigParser()
        config_parser.read(self.target_path)
        for section, section_config in self.config.items():
            for key, value in section_config.items():
                self.assertEqual(config_parser[section][key], str(value))


class TestConfigRead(unittest.TestCase):
    def setUp(self):
        self.sample_config_path = 'resources/sample_config.ini'

    def test_read_valid_file(self):
        config_parser = config_read(self.sample_config_path)
        self.assertIsInstance(config_parser, configparser.ConfigParser)

        self.assertEqual(set(config_parser.sections()), {"section1", "section2"})

        self.assertEqual(dict(config_parser["DEFAULT"]), {"key3": "value3", "keyc": "0"})
        self.assertEqual(dict(config_parser["section1"]), {'key1': 'value1', 'key2': '1', 'key3': 'value3', 'keyc': '0'})
        self.assertEqual(dict(config_parser["section2"]), {'key3': 'value3', 'keya': '2.44', 'keyb': 'True', 'keyc': 'None'})

        self.assertEqual(config_parser['section1'].get('key1'), 'value1')
        self.assertEqual(config_parser['section1'].getint('key2'), 1)
        self.assertAlmostEqual(config_parser['section2'].getfloat('keya'), 2.44)
        self.assertEqual(config_parser['section2'].getboolean('keyb'), True)
        self.assertEqual(config_parser['section2'].get('keyc'), 'None')
        self.assertEqual(config_parser['section1'].get('key9'), None)

        with self.assertRaises(KeyError):
            config_parser['section3'].get('key3')

    def test_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            config_read('resources/non_existent_file.ini')

    def test_path_is_directory(self):
        with self.assertRaises(NotADirectoryError):
            config_read('resources/test_module_use')

    def test_read_invalid_config(self):
        with open('resources/temp.ini', 'w') as f:
            f.write("not a config file")
        with self.assertRaises(Exception):
            config_read('resources/temp.ini')
        os.remove('resources/temp.ini')

    def tearDown(self):
        pass


if __name__ == "__main__":
    unittest.main()
