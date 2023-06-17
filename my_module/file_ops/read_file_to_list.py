import os
from typing import List, Union


# noinspection PyShadowingNames
def read_file_to_list(target_path: Union[str, os.PathLike]) -> List[str]:
    """
    读取文本文件中的内容，并将其存储成列表。

    :type target_path: Union[str, os.PathLike]
    :param target_path: 文本文件的路径，可以是字符串或 os.PathLike 对象。
    :rtype: List[str]
    :return: 成功时返回文本内容列表。
    :raise FileNotFoundError: 如果路径不存在，抛出 FileNotFoundError。
    :raise NotADirectoryError: 如果路径是一个目录，而不是文件，抛出 NotADirectoryError。
    :raise PermissionError: 如果无法访问文件，可能是因为权限错误，抛出 PermissionError。
    :raise ValueError: 如果无法解码文件，抛出 ValueError。
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception。
    """
    if not os.path.exists(target_path):
        raise FileNotFoundError(f"The file '{target_path}' does not exist.")
    if not os.path.isfile(target_path):
        raise NotADirectoryError(f"'{target_path}' is not a valid file.")

    try:
        with open(target_path, 'r', encoding="utf-8") as file:
            return [line.strip() for line in file]
    except PermissionError:
        raise PermissionError(f"Cannot access file '{target_path}', permission denied.")
    except UnicodeDecodeError:
        raise ValueError(f"Cannot decode file '{target_path}', please check whether it is in 'UTF-8' format.")
    except Exception as e:
        raise Exception(f"An error occurred while reading the file '{target_path}': {e}")
