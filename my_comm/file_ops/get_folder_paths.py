import os
from typing import List, Union

def get_folder_paths(target_path: Union[str, os.PathLike]) -> List[str]:
    """
    获取目标目录下扫描到的所有文件夹路径

    :type target_path: Union[str, os.PathLike]
    :param target_path: 目标目录的路径，可以是字符串或 os.PathLike 对象。
    :rtype: List[str]
    :return: 文件夹路径列表。
    :raise FileNotFoundError: 如果路径不存在，抛出 FileNotFoundError。
    :raise NotADirectoryError: 如果路径不是一个有效的目录，抛出 NotADirectoryError。
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception。
    """
    target_path = os.path.normpath(target_path)
    if not os.path.exists(target_path):
        raise FileNotFoundError(f"The path '{target_path}' does not exist.")
    if not os.path.isdir(target_path):
        raise NotADirectoryError(f"'{target_path}' is not a valid directory.")

    try:
        return [os.path.normpath(os.path.join(root, dir_name)) for root, dirs, _ in os.walk(target_path) for dir_name in dirs]
    except Exception as e:
        raise Exception(f"An error occurred while retrieving folder paths: {e}")

if __name__ == '__main__':
    target_path = r'resources'
    try:
        folder_paths_list = get_folder_paths(target_path=target_path)
        print(folder_paths_list)
    except Exception as e:
        print(f"An error occurred: {e}")
