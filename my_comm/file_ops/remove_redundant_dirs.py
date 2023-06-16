import os
from typing import List, Union
import uuid


def remove_redundant_dirs(target_path: Union[str, os.PathLike]) -> List[str]:
    """
    移除冗余目录结构。

    如果一个目录只有一个子目录，且该子目录的名称与父目录名称相同，
    且父目录没有其他文件，则删除子目录，并将其内容移至父目录。

    :type target_path: Union[str, os.PathLike]
    :param target_path: 需要进行处理的目录路径。
    :rtype: List[str]
    :return: 一个列表，包含所有被移除的子目录的路径。
    :raise FileNotFoundError: 如果路径不存在，抛出 FileNotFoundError。
    :raise NotADirectoryError: 如果路径不是一个有效的目录，抛出 NotADirectoryError。
    :raise Exception: 如果在处理过程中出现其他问题，抛出一般性的 Exception。
    """
    if not os.path.exists(target_path):
        raise FileNotFoundError(f"The path '{target_path}' does not exist.")

    if not os.path.isdir(target_path):
        raise NotADirectoryError(f"'{target_path}' is not a valid directory.")

    removed_dirs = []

    try:
        for subdir in os.scandir(target_path):
            if subdir.is_dir():
                subdir_path = subdir.path
                sub_subdirs = [entry for entry in os.scandir(subdir_path) if entry.is_dir()]

                if len(sub_subdirs) == 1 and sub_subdirs[0].name == os.path.basename(subdir_path):
                    sub_subdir_path = sub_subdirs[0].path
                    parent_files = [entry for entry in os.scandir(subdir_path) if entry.is_file()]

                    if not parent_files:
                        temp_dir = os.path.join(subdir_path, f"{os.path.basename(sub_subdir_path)}_{uuid.uuid4()}")
                        os.rename(sub_subdir_path, temp_dir)

                        for item in os.scandir(temp_dir):
                            os.rename(item.path, os.path.join(subdir_path, item.name))

                        if not any(os.scandir(temp_dir)):
                            os.rmdir(temp_dir)

                        removed_dirs.append(os.path.normpath(sub_subdir_path))

    except Exception as e:
        raise Exception(f"An error occurred while removing redundant directories: {e}")

    return removed_dirs


if __name__ == '__main__':
    target_path = r'resources'
    try:
        removed_dirs = remove_redundant_dirs(target_path=target_path)
        print(f"Removed directories: {removed_dirs}")
    except Exception as e:
        print(f"An error occurred: {e}")
