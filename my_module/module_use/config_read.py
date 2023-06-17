import configparser
from pathlib import Path
from typing import Optional


# noinspection PyShadowingNames
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
    target_path = Path(target_path)

    if not target_path.exists():
        raise FileNotFoundError(f"The path '{target_path}' does not exist.")
    if target_path.is_dir():
        raise NotADirectoryError(f"The path '{target_path}' is a directory.")

    config_parser = configparser.ConfigParser()
    try:
        with open(target_path, 'r') as f:
            config_parser.read_file(f)
    except Exception as e:
        raise Exception(f"An error occurred while reading the config file {target_path}: {e}")

    return config_parser
