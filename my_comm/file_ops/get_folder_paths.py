import os
from typing import List


def get_folder_paths(target_path: str) -> List[str]:
    """
    获取目标目录下扫描到的所有文件夹路径

    :param target_path: 目标目录
    :return: 文件夹路径列表
    :raise ValueError: 如果路径不存在或者不是一个有效的目录，抛出 ValueError
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception
    """
    if not os.path.exists(target_path):
        raise ValueError(f"路径 '{target_path}' 不存在")

    if not os.path.isdir(target_path):
        raise ValueError(f"'{target_path}' 不是一个有效的目录")

    folder_paths = []

    try:
        for root, dirs, _ in os.walk(target_path):
            for dir in dirs:
                full_path = os.path.join(root, dir)
                folder_paths.append(full_path)
        return folder_paths
    except Exception as e:
        raise Exception(f"在获取文件夹路径时发生错误: {e}")


if __name__ == '__main__':
    目标目录 = r'resources'
    try:
        目录路径列表 = get_folder_paths(target_path=目标目录)
        print(目录路径列表)
    except Exception as e:
        print(e)
