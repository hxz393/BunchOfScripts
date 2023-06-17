import os
from typing import List, Union


def get_file_paths_by_type(target_path: Union[str, os.PathLike], type_list: List[str]) -> List[str]:
    """
    获取指定路径下特定类型文件的路径列表。

    :type target_path: Union[str, os.PathLike]
    :param target_path: 需要搜索的路径
    :type type_list: List[str]
    :param type_list: 拿来匹配的文件类型列表
    :rtype: List[str]
    :return: 匹配的文件路径列表
    :raise FileNotFoundError: 如果指定路径不存在会抛出此异常
    :raise ValueError: 如果文件类型列表为空会抛出此异常
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception。
    """
    if not os.path.exists(target_path):
        raise FileNotFoundError(f"The path '{target_path}' does not exist.")
    if not os.path.isdir(target_path):
        raise NotADirectoryError(f"'{target_path}' is not a valid directory.")
    if not type_list:
        raise ValueError(f"The file type list is empty.")

    type_list = [file_type.lower() for file_type in type_list]

    try:
        return [os.path.normpath(os.path.join(root, file)) for root, _, files in os.walk(target_path) for file in files if os.path.splitext(file)[1].lower() in type_list]
    except Exception as e:
        raise Exception(f"An error occurred while retrieving file paths: {e}")
