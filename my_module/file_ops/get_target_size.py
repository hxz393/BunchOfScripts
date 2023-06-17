import os
from typing import Union
from concurrent.futures import ThreadPoolExecutor


# noinspection PyShadowingNames
def get_file_size(file_path: Union[str, os.PathLike]) -> int:
    """
    获取单个文件的大小。

    :type file_path: Union[str, os.PathLike]
    :param file_path: 单个文件的路径，可以是 str 或 os.PathLike 对象。
    :rtype: int
    :return: 文件的大小（字节数）。
    :raise FileNotFoundError: 如果路径不存在，抛出 FileNotFoundError。
    :raise Exception: 如果在处理过程中出现其他问题，抛出一般性的 Exception。
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file '{file_path}' does not exist.")
    try:
        return os.path.getsize(file_path)
    except Exception as e:
        raise Exception(f"An error occurred while getting file size: {e}")


# noinspection PyShadowingNames
def get_target_size(target_path: Union[str, os.PathLike]) -> int:
    """
    获取目标文件或文件夹的大小。

    :type target_path: Union[str, os.PathLike]
    :param target_path: 文件或文件夹的路径，可以是 str 或 os.PathLike 对象。
    :rtype: int
    :return: 文件或文件夹的大小（字节数）。
    :raise FileNotFoundError: 如果路径不存在，抛出 FileNotFoundError。
    :raise Exception: 如果在处理过程中出现其他问题，抛出一般性的 Exception。
    """
    if not os.path.exists(target_path):
        raise FileNotFoundError(f"The path '{target_path}' does not exist.")
    try:
        if os.path.isfile(target_path):
            return get_file_size(target_path)
        elif os.path.isdir(target_path):
            with ThreadPoolExecutor() as executor:
                sizes = executor.map(get_file_size, (os.path.join(dirpath, f) for dirpath, dirnames, filenames in os.walk(target_path) for f in filenames))
            return sum(sizes)
        else:
            raise ValueError(f"'{target_path}' is not a file or a directory.")
    except Exception as e:
        raise Exception(f"An error occurred while getting target size: {e}")
