import os
from typing import List, Union

def get_subdirectories(target_path: Union[os.PathLike, str]) -> List[str]:
    """
    获取目标目录下的第一级目录路径列表。

    :type target_path: Union[os.PathLike, str]
    :param target_path: 检测目录，可以是 str 或 os.PathLike 对象。
    :rtype: List[str]
    :return: 文件夹路径列表。
    :raise ValueError: 如果路径不是一个有效的目录，或者路径不存在，抛出 ValueError。
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception。
    """

    if not os.path.exists(target_path):
        raise ValueError(f"The path '{target_path}' does not exist.")

    if not os.path.isdir(target_path):
        raise ValueError(f"'{target_path}' is not a valid directory.")

    try:
        return [entry.path for entry in os.scandir(target_path) if entry.is_dir()]
    except Exception as e:
        raise Exception(f"An error occurred while getting subdirectories: {e}")


if __name__ == '__main__':
    target_dir = 'resources/'
    try:
        dir_list = get_subdirectories(target_path=target_dir)
        print(dir_list)
    except Exception as e:
        print(f"An error occurred: {e}")
