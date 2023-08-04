"""
这是一个Python文件，包含两个函数：`remove_permissions`和`remove_target`。

函数 `remove_permissions` 用于移除目标路径的权限，并调用指定的函数。它接受三个参数：`func`，`path`和`_`。`func`是要调用的函数，它将`path`作为参数；`path`是目标路径；`_`是用于异常处理的错误信息。在调用`func`之前，它会先更改`path`的权限。

函数 `remove_target` 用于删除指定的文件或目录。它接受一个参数：`path`，这是要删除的文件或目录的路径。如果`path`存在且是一个目录，它将使用`shutil.rmtree`来删除目录，如果遇到权限错误，会调用`remove_permissions`来处理。如果`path`是一个文件，它将使用`Path.unlink`来删除文件。如果`path`既不是目录也不是文件，它会记录错误信息并返回`None`。

这个模块主要用于文件系统操作，包括更改文件或目录权限，以及删除文件或目录。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import logging
import traceback
import os
import shutil
import stat
from pathlib import Path
from typing import Callable, Any, Union, Optional

logger = logging.getLogger(__name__)


def remove_permissions(func: Callable[[Path], Any], path: Path, _: Any) -> None:
    """
    移除目标路径的权限并调用指定的函数。

    :param func: 要调用的函数。
    :type func: Callable[[Path], Any]
    :param path: 目标路径。
    :type path: Path
    :param _: 用于异常处理的错误信息。
    :type _: Any
    """
    path.chmod(0o777)
    os.chmod(path, stat.S_IWRITE)
    func(path)


def remove_target(path: Union[str, Path]) -> Optional[Path]:
    """
    删除指定文件或目录。

    :param path: 要删除的文件或目录的路径。
    :type path: Union[str, Path]
    :return: 如果操作成功则返回删除的文件或目录的路径，遇到错误则返回 None。
    :rtype: Optional[Path]
    """
    path = Path(path)

    try:
        if not path.exists():
            return None

        if path.is_dir():
            shutil.rmtree(path, onerror=remove_permissions)
            return path
        elif path.is_file():
            path.unlink()
            return path
        else:
            logger.error(f"'{path}' is neither a file nor a directory.")
            return None
    except PermissionError:
        remove_permissions(lambda x: None, path, None)
        path.unlink()
        return path
    except Exception as e:
        logger.error(f"An error occurred while removing path '{path}': {e}\n{traceback.format_exc()}")
        return None
