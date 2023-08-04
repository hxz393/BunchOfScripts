"""
这是一个Python文件，包含一个函数：`remove_target_matched`。

函数 `remove_target_matched` 用于删除目标路径下与给定匹配列表中任一名称完全匹配的文件或文件夹。它接受两个参数：`target_path`和`match_list`。`target_path`是指定的目标路径，可以是字符串或os.PathLike对象。`match_list`是需要匹配的目标列表，列表中的每个元素是一个字符串。该函数首先检查`target_path`是否存在以及`match_list`是否为空。然后，它会对`target_path`进行递归遍历，寻找与`match_list`中任一元素名称完全匹配的文件或目录，并删除这些匹配的文件或目录。如果成功，返回一个包含被删除路径的列表，否则返回None。

这个模块主要用于文件系统操作，包括删除符合条件的文件或目录。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import logging
import traceback
import os
from typing import List, Union, Optional

from file_ops import remove_target

logger = logging.getLogger(__name__)


def remove_target_matched(target_path: Union[str, os.PathLike], match_list: List[str]) -> Optional[List[str]]:
    """
    删除目标路径下与给定匹配列表中任一名字完全匹配的文件或文件夹。

    :param target_path: 指定的目标路径，可是是字符串或 os.PathLike 对象。
    :type target_path: Union[str, os.PathLike]
    :param match_list: 需要匹配的目标列表，列表中的每个元素是一个字符串。
    :type match_list: List[str]
    :return: 一个包含被删除路径的列表，如果遇到错误则返回 None。
    :rtype: Optional[List[str]]
    """
    if not os.path.exists(target_path):
        logger.error(f"The path '{target_path}' does not exist.")
        return None

    if not match_list:
        logger.error(f"Match list is empty.")
        return None

    try:
        match_list_lower = [item.lower() if isinstance(item, str) else item for item in match_list]
        matched_paths = [
            os.path.normpath(os.path.join(root, file))
            for root, dirs, files in os.walk(target_path)
            for file in files + dirs
            if file.lower() in match_list_lower
        ]
        for path in matched_paths:
            remove_target(path)
        return matched_paths
    except Exception as e:
        logger.error(f"An error occurred while removing matched targets. Error message: {e}\n{traceback.format_exc()}")
        return None
