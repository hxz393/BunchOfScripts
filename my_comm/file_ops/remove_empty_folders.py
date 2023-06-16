import os
from typing import Union, List


def remove_empty_folders(target_path: Union[str, os.PathLike]) -> List[str]:
    """
    删除指定路径下搜索到的所有空目录，并返回被删除的目录路径列表。

    :param target_path: 需要处理的目录路径，可以是字符串或 Path 对象。
    :type target_path: Union[str, Path]
    :raise FileNotFoundError: 如果路径不存在，抛出 FileNotFoundError。
    :raise NotADirectoryError: 如果路径不是一个目录，抛出 NotADirectoryError。
    :raise OSError: 如果在处理过程中出现其他错误，抛出 OSError。
    :return: 一个列表，包含所有被删除的空目录的路径。
    :rtype: List[str]
    """
    removed_dirs = []

    try:
        entries = list(os.scandir(target_path))
        for entry in entries:
            if entry.is_dir(follow_symlinks=False):
                removed_dirs.extend(remove_empty_folders(entry.path))

        if not entries:
            os.rmdir(target_path)
            removed_dirs.append(str(target_path))
    except FileNotFoundError:
        raise FileNotFoundError(f"The path '{target_path}' does not exist.")
    except NotADirectoryError:
        raise NotADirectoryError(f"'{target_path}' is not a valid directory.")
    except OSError as e:
        raise OSError(f"Cannot delete directory '{target_path}': {str(e)}")

    return removed_dirs


if __name__ == '__main__':
    try:
        target_directory = r'resources/'
        removed_directories = remove_empty_folders(target_path=target_directory)
        print(f"Removed directories: {removed_directories}")
    except Exception as e:
        print(f"An error occurred: {e}")
