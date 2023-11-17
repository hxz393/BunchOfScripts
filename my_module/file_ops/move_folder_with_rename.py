"""
这是一个Python文件，包含了一个名为`move_folder_with_rename`的函数。该函数可以将源路径上的文件或文件夹移动到目标路径。如果目标路径上已经存在了同名的文件或文件夹，那么会对其进行重命名。

函数接受两个参数，`source_path`和`target_path`。`source_path`是你想要移动的源文件或文件夹的路径，`target_path`是你想要将文件或文件夹移动到的目标位置的路径。这两个参数都可以是字符串或`Path`对象。

在开始移动之前，函数会检查源路径和目标路径是否存在。如果源文件或文件夹不存在，或者目标位置的父目录不存在，函数会记录错误并返回None。

此函数还使用了`rename_target_if_exist`函数，它会检查目标位置是否已经存在同名的文件或文件夹，如果有，就对其进行重命名。

最后，函数使用`shutil.move`来移动文件或文件夹。如果在移动过程中出现任何错误，函数会记录错误并返回None。如果移动成功，函数会返回移动后的目标路径。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393。保留所有权利。
"""
import logging

from pathlib import Path
from shutil import move
from typing import Union, Optional

from my_module.file_ops.rename_target_if_exist import rename_target_if_exist

logger = logging.getLogger(__name__)


def move_folder_with_rename(source_path: Union[str, Path], target_path: Union[str, Path]) -> Optional[Union[str, Path]]:
    """
    将文件或文件夹移动到目标位置，如果目标位置已存在相同名称的文件或文件夹，则重命名。

    :param source_path: 源文件或文件夹路径
    :type source_path: Union[str, Path]
    :param target_path: 想要移到的目标位置路径
    :type target_path: Union[str, Path]
    :return: 移动并可能被重命名后的目标文件或文件夹的路径，如果发生错误则返回None
    :rtype: Optional[Union[str, Path]]
    """
    source_path = Path(source_path)
    target_path = Path(target_path)

    if not source_path.exists():
        logger.error(f"The source file or folder '{source_path}' does not exist.")
        return None

    if not target_path.parent.exists():
        logger.error(f"The parent directory of the target location '{target_path.parent}' does not exist.")
        return None

    target_path = rename_target_if_exist(target_path)

    try:
        move(str(source_path), str(target_path))
    except Exception:
        logger.exception(f"An error occurred while moving the file or folder. Error message")
        return None

    return target_path
