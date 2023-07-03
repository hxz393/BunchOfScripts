"""
这是一个Python文件，包含一个函数：`config_read`。

`config_read`函数的目标是从指定的路径读取配置文件。如果文件成功读取，它将返回一个`configparser.ConfigParser`对象。如果读取失败，它将返回`None`。

这个函数接受一个参数：
- `target_path`：需要读取的配置文件的路径，可以是字符串或`pathlib.Path`对象。

此文件依赖于以下Python库：
- `configparser`
- `pathlib`
- `typing`
- `logging`

函数使用了日志记录器记录任何在读取过程中发生的错误。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import configparser
import logging
from pathlib import Path
from typing import Optional, Union

logger = logging.getLogger(__name__)


def config_read(target_path: Union[str, Path]) -> Optional[configparser.ConfigParser]:
    """
    从指定的路径读取配置文件。

    :param target_path: 配置文件的路径。
    :type target_path: Union[str, Path]
    :return: 如果文件读取成功，返回一个 configparser.ConfigParser 对象；如果文件读取失败，返回 None。
    :rtype: Optional[configparser.ConfigParser]
    """
    target_path = Path(target_path)

    try:
        if not target_path.exists():
            logger.error(f"The path '{target_path}' does not exist.")
            return None
        if target_path.is_dir():
            logger.error(f"The path '{target_path}' is a directory.")
            return None

        config_parser = configparser.ConfigParser()
        with open(target_path, 'r', encoding="utf-8") as f:
            config_parser.read_file(f)
    except Exception as e:
        logger.error(f"An error occurred while reading the config file {target_path}: {e}")
        return None

    return config_parser
