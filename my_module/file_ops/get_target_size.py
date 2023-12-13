"""
这个Python文件包含两个函数，`get_file_size`和`get_target_size`。这两个函数都是为了获取文件或者文件夹的大小。

`get_file_size`函数获取指定文件的大小（以字节为单位）。接受一个参数，即文件路径，可以是字符串或者os.PathLike对象。如果给定的路径不存在，或者在尝试获取文件大小时发生错误，此函数将记录错误并返回None。

`get_target_size`函数可以获取文件或者文件夹的大小（以字节为单位）。它接受一个参数，即文件或文件夹的路径，可以是字符串或者os.PathLike对象。如果给定的路径不存在，或者在尝试获取大小时发生错误，此函数将记录错误并返回None。

这个文件中使用了`concurrent.futures`库中的`ThreadPoolExecutor`，这是因为获取一个包含许多文件和子文件夹的目录的大小可能会非常慢，所以使用了多线程来提高速度。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393。保留所有权利。
"""

import logging

import os
from concurrent.futures import ThreadPoolExecutor
from typing import Union, Optional

logger = logging.getLogger(__name__)


def get_file_size(file_path: Union[str, os.PathLike]) -> Optional[int]:
    """
    获取单个文件的大小。

    :param file_path: 单个文件的路径，可以是 str 或 os.PathLike 对象。
    :type file_path: Union[str, os.PathLike]
    :return: 文件的大小（字节数）或者在有错误时返回None。
    :rtype: Optional[int]
    """
    if not os.path.exists(file_path):
        logger.error(f"The file '{file_path}' does not exist.")
        return None
    try:
        return os.path.getsize(file_path)
    except Exception:
        logger.exception("An error occurred while getting file size")
        return None


def get_target_size(target_path: Union[str, os.PathLike]) -> Optional[int]:
    """
    获取目标文件或文件夹的大小。

    :param target_path: 文件或文件夹的路径，可以是 str 或 os.PathLike 对象。
    :type target_path: Union[str, os.PathLike]
    :return: 文件或文件夹的大小（字节数）或者在有错误时返回None。
    :rtype: Optional[int]
    """
    if not os.path.exists(target_path):
        logger.error(f"The path '{target_path}' does not exist.")
        return None
    try:
        if os.path.isfile(target_path):
            return get_file_size(target_path)
        elif os.path.isdir(target_path):
            with ThreadPoolExecutor() as executor:
                sizes = executor.map(get_file_size, (os.path.join(dir_path, f) for dir_path, dir_names, file_names in os.walk(target_path) for f in file_names))
            return sum(sizes) if sizes else None
        else:
            logger.error(f"'{target_path}' is not a file or a directory.")
            return None
    except Exception:
        logger.exception("An error occurred while getting target size")
        return None
