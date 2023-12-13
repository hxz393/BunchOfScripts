"""
这是一个Python文件，它包含一个名为`remove_empty_dirs`的函数，该函数用于删除指定路径下所有空目录，并返回被删除的目录路径列表。

该函数接受一个参数`target_path`，这是需要处理的目录路径，可以是字符串或`os.PathLike`对象。函数会在指定路径下寻找所有的空目录并删除。删除的目录路径将被添加到列表`removed_dirs`中。

删除空目录的过程中，可能会出现一些错误。函数会捕获和处理这些错误，例如：路径不存在（`FileNotFoundError`），指定的路径不是一个有效的目录（`NotADirectoryError`），无法删除目录（`OSError`），以及其它可能的异常（`Exception`）。当这些错误发生时，函数会记录相应的错误信息，并返回`None`。

这个函数可以用于清理文件系统中的空目录，使文件系统保持整洁。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import logging

import os
import stat
from typing import Union, List, Optional

logger = logging.getLogger(__name__)


def remove_empty_dirs(target_path: Union[str, os.PathLike]) -> Optional[List[str]]:
    """
    删除指定路径下搜索到的所有空目录，并返回被删除的目录路径列表。

    :param target_path: 需要处理的目录路径，可以是字符串或 os.PathLike 对象。
    :type target_path: Union[str, os.PathLike]
    :return: 成功时返回一个列表，包含所有被删除的空目录的路径，如果遇到错误则返回None。
    :rtype: Optional[List[str]]
    """
    removed_dirs = []

    try:
        entries = list(os.scandir(target_path))
        for entry in entries:
            if entry.is_dir(follow_symlinks=False):
                removed_dirs.extend(remove_empty_dirs(entry.path))
        if not entries:
            os.chmod(target_path, stat.S_IWRITE)
            os.rmdir(target_path)
            removed_dirs.append(str(target_path))
    except Exception:
        logger.exception("An error occurred while deleting empty directories")
        return None

    return removed_dirs
