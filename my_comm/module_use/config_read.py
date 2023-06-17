import os
import configparser
from typing import Optional

def config_read(target_path: str) -> Optional[configparser.ConfigParser]:
    """
    从指定的路径读取配置文件。

    :param target_path: 配置文件的路径。
    :type target_path: str
    :rtype: Optional[configparser.ConfigParser]
    :raise FileNotFoundError: 如果路径不存在，抛出 FileNotFoundError。
    :raise NotADirectoryError: 如果路径是一个目录，抛出 NotADirectoryError。
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception。
    :return: 如果文件读取成功，返回一个 configparser.ConfigParser 对象；如果文件读取失败，返回 None。
    """
    target_path = os.path.normpath(target_path)

    if not os.path.exists(target_path):
        raise FileNotFoundError(f"The path '{target_path}' does not exist.")
    if os.path.isdir(target_path):
        raise IsADirectoryError(f"The path '{target_path}' is a directory.")

    config_parser = configparser.ConfigParser()
    try:
        with open(target_path, 'r') as f:
            config_parser.read_file(f)
    except Exception as e:
        raise Exception(f"An error occurred while reading the config file {target_path}: {e}")

    return config_parser

if __name__ == '__main__':
    try:
        config_parser = config_read(r"config/config.ini")
        if config_parser is not None:
            for section in config_parser.sections():
                print(f"{section}:")
                for key, value in config_parser.items(section):
                    print(f"  {key} = {value}")
            print(config_parser['section1'].get('key1'))
            print(config_parser['section1'].getint('key2'))
            print(config_parser['section2'].getfloat('keya'))
            print(config_parser['section2'].getboolean('keyb'))
            print(config_parser['section1'].getboolean('key2'))
            print(config_parser['section1'].get('key3'))
            print(config_parser['section2'].get('keyc'))
    except Exception as e:
        print(f"An error occurred: {e}")
