"""
这是一个Python文件，其中包含一个名为`remove_redundant_dirs`的函数，用于移除冗余的目录结构。

当一个目录只包含一个同名的子目录，并且没有其他文件时，该函数会删除这个子目录，并将其内容移至父目录。这个操作被视为冗余目录结构的移除。

函数的输入参数`target_path`是需要进行处理的目录路径，可以是字符串或`os.PathLike`对象。函数会递归遍历该路径下的所有子目录，并检查它们是否满足移除的条件。

如果满足条件，那么会先将子目录改名为一个临时名，然后将该目录下的所有文件或目录移至父目录，最后删除这个临时目录。

所有被移除的子目录路径会被添加到列表`removed_dirs`中，这个列表会作为函数的返回值。如果在处理过程中遇到任何错误，函数会记录相应的错误信息，并返回`None`。

该函数主要用于文件系统清理，通过移除冗余的目录结构，使得文件的组织结构更加紧凑和清晰。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import logging
import traceback
import os
import stat
import uuid
from typing import List, Union, Optional

logger = logging.getLogger(__name__)


def remove_redundant_dirs(target_path: Union[str, os.PathLike]) -> Optional[List[str]]:
    """
    移除冗余目录结构。

    如果一个目录只有一个子目录，且该子目录的名称与父目录名称相同，
    且父目录没有其他文件，则删除子目录，并将其内容移至父目录。

    :param target_path: 需要进行处理的目录路径。
    :type target_path: Union[str, os.PathLike]
    :return: 成功时返回一个列表，包含所有被移除的子目录的路径。如果遇到错误则返回 None。
    :rtype: Optional[List[str]]
    """
    removed_dirs = []

    try:
        if not os.path.exists(target_path):
            logger.error(f"The path '{target_path}' does not exist.")
            return None

        if not os.path.isdir(target_path):
            logger.error(f"'{target_path}' is not a valid directory.")
            return None

        dirs_to_process = [target_path]

        while dirs_to_process:
            current_dir = dirs_to_process.pop()

            for subdir in os.scandir(current_dir):
                if not subdir.is_dir():
                    continue

                dirs_to_process.append(subdir.path)

                sub_subdirs = [entry for entry in os.scandir(subdir.path) if entry.is_dir()]

                if len(sub_subdirs) != 1 or sub_subdirs[0].name != os.path.basename(subdir.path):
                    continue

                sub_subdir_path = sub_subdirs[0].path
                parent_files = [entry for entry in os.scandir(subdir.path) if entry.is_file()]

                if parent_files:
                    continue

                temp_dir = os.path.join(subdir.path, f"{os.path.basename(sub_subdir_path)}_{uuid.uuid4()}")
                os.rename(sub_subdir_path, temp_dir)

                for item in os.scandir(temp_dir):
                    os.rename(item.path, os.path.join(subdir.path, item.name))

                if not any(os.scandir(temp_dir)):
                    os.chmod(temp_dir, stat.S_IWRITE)
                    os.rmdir(temp_dir)

                removed_dirs.append(os.path.normpath(sub_subdir_path))
    except Exception as e:
        logger.error(f"An error occurred while removing redundant directories: {e}\n{traceback.format_exc()}")
        return None

    return removed_dirs
