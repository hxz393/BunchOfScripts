import os
from typing import List, Union


def get_file_paths(target_path: Union[str, os.PathLike]) -> List[str]:
    """
    获取目标目录下所有文件的路径。

    :param target_path: 目标目录的路径，可以是字符串或 os.PathLike 对象。
    :raise ValueError: 如果路径不存在或者不是一个有效的目录，抛出 ValueError。
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception。
    :return: 一个列表，包含所有文件的路径。
    """
    if not os.path.exists(target_path):
        raise ValueError(f"路径 '{target_path}' 不存在")
    if not os.path.isdir(target_path):
        raise ValueError(f"'{target_path}' 不是一个有效的目录")

    file_paths = []
    try:
        for root, _, files in os.walk(target_path):
            for file in files:
                file_paths.append(os.path.join(root, file))
    except Exception as e:
        raise Exception(f"在获取文件路径时发生错误: {e}")

    return file_paths


if __name__ == '__main__':
    目标路径 = r'resources'
    try:
        文件路径列表 = get_file_paths(目标路径)
        print(文件路径列表)
    except Exception as e:
        print(f"发生错误：{e}")
