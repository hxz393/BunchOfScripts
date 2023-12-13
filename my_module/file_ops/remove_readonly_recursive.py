"""
这是一个Python文件，其中包含两个主要函数：remove_readonly 和 remove_readonly_recursive。

函数 remove_readonly 的主要功能是移除给定文件或文件夹的只读属性。它接受一个参数 target_path，这是需要操作的文件或文件夹的路径。函数首先检查 target_path 是否存在，如果不存在则记录错误并返回 None。如果 target_path 存在，函数将获取它的属性，然后使用 os.chmod 移除只读属性。如果操作成功，函数返回 True，如果发生错误则记录错误并返回 None。

函数 remove_readonly_recursive 的主要功能是递归地移除给定目录及其所有子目录和文件的只读属性。它接受一个参数 target_dir，这是需要操作的目录的路径。函数首先检查 target_dir 是否存在，如果不存在则记录错误并返回 None。如果 target_dir 存在，函数将对目录及其所有子目录和文件调用 remove_readonly 函数。如果操作成功，函数返回 True，如果发生错误则记录错误并返回 None。

这个模块主要用于处理文件和目录的只读属性，包括移除单个文件或目录的只读属性，以及递归地移除目录及其所有子目录和文件的只读属性。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""

import logging
import os

from typing import Optional, Union

import win32api
import win32con

logger = logging.getLogger(__name__)


def remove_readonly(target_path: Union[str, os.PathLike]) -> Optional[bool]:
    """
    取消指定路径下的文件或文件夹的只读属性。

    :param target_path: 要处理的文件或文件夹的路径，可以是字符串或 os.PathLike 对象。
    :type target_path: Union[str, os.PathLike]
    :rtype: Optional[bool]
    :return: 如果成功取消只读属性则返回 True，如果发生错误则返回 None。
    """
    try:
        if not os.path.exists(target_path):
            logger.error(f"The path '{target_path}' does not exist.")
            return None
        # 使用win32api设置文件属性
        win32api.SetFileAttributes(target_path, win32con.FILE_ATTRIBUTE_NORMAL)
        return True
    except Exception:
        logger.exception("An error occurred while removing read-only attribute")
        return None


def remove_readonly_recursive(target_dir: Union[str, os.PathLike]) -> Optional[bool]:
    """
    取消指定目录以及其所有子目录和文件的只读属性。

    :param target_dir: 要处理的目录的路径，可以是字符串或 os.PathLike 对象。
    :type target_dir: Union[str, os.PathLike]
    :rtype: Optional[bool]
    :return: 如果成功取消只读属性则返回 True，如果发生错误则返回 None。
    """
    try:
        if not os.path.exists(target_dir):
            logger.error(f"The path '{target_dir}' does not exist.")
            return None
        if not os.path.isdir(target_dir):
            logger.error(f"'{target_dir}' is not a valid directory.")
            return None
        # 先设置自己的属性，然后遍历子目录和文件
        remove_readonly(target_dir)
        for root, dirs, files in os.walk(target_dir):
            for dir in dirs:
                remove_readonly(os.path.join(root, dir))
            for file in files:
                remove_readonly(os.path.join(root, file))
        return True
    except Exception:
        logger.exception("An error occurred while removing read-only attribute recursively")
        return None
