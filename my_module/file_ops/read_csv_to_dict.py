"""
这是一个用于数据处理的Python模块，包含用于读取CSV文件并将其内容转换为字典列表的功能。

此模块中的主要函数是 `read_csv_to_dict`。此函数接受一个参数 `target_path`，这可以是字符串或 `os.PathLike` 对象，指向要读取的CSV文件。该函数会尝试读取指定路径的CSV文件，并将其内容转换为字典列表。在读取文件时，该函数会检查文件的存在性以及是否可以正常打开和读取。如果读取成功，它将返回包含文件数据的字典列表；如果在任何步骤中出现错误，它将记录错误并返回 `None`。

此模块的设计旨在简化CSV文件的读取过程，并支持各种可能的错误处理，确保数据处理的健壮性和可靠性。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""

import csv
import logging
import os

from typing import List, Dict, Optional, Union

logger = logging.getLogger(__name__)


def read_csv_to_dict(target_path: Union[str, os.PathLike]) -> Optional[List[str]]:
    """
    从 CSV 文件中读取数据并转换成字典列表。

    :param target_path: 要读取的CSV文件的路径。
    :type target_path: str
    :rtype: Optional[List[str]]
    :return: 包含文件数据的字典列表，或在发生错误时返回 None。
    """
    if not os.path.exists(target_path):
        logger.error(f"The file '{target_path}' does not exist.")
        return None
    if not os.path.isfile(target_path):
        logger.error(f"'{target_path}' is not a valid file.")
        return None

    try:
        with open(target_path, 'r', encoding='utf-8-sig') as csvfile:
            dialect = csv.Sniffer().sniff(csvfile.read(1024))
            csvfile.seek(0)
            reader = csv.DictReader(csvfile, dialect=dialect)
            return [row for row in reader]
    except PermissionError:
        logger.error(f"Cannot access file '{target_path}', permission denied.")
        return None
    except UnicodeDecodeError:
        logger.error(f"Cannot decode file '{target_path}', please check whether it is in 'UTF-8' format.")
        return None
    except Exception:
        logger.exception(f"An error occurred while reading the file '{target_path}'")
        return None
