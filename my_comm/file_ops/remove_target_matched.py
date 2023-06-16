import os
from typing import List, Union

from my_comm.file_ops.remove_target import remove_target


def remove_target_matched(target_path: Union[str, os.PathLike], match_list: List[str]) -> List[str]:
    """
    删除目标路径下与给定匹配列表中任一名字完全匹配的文件或文件夹。

    :param target_path: 指定的目标路径，可是是字符串或 os.PathLike 对象。
    :type target_path: Union[str, os.PathLike]
    :param match_list: 需要匹配的目标列表，列表中的每个元素是一个字符串。
    :type match_list: List[str]
    :rtype: List[str]
    :return: 一个包含被删除路径的列表。
    :raise FileNotFoundError: 如果指定的目标路径不存在，则抛出此异常。
    :raise ValueError: 如果匹配目标列表为空，则抛出此异常。
    :raise Exception: 如果在处理过程中遇到任何其它问题，抛出一般性的 Exception。
    """
    if not os.path.exists(target_path):
        raise FileNotFoundError(f"The path '{target_path}' does not exist.")

    if not match_list:
        raise ValueError(f"Match list is empty.")

    try:
        matched_paths = [
            os.path.normpath(os.path.join(root, file))
            for root, dirs, files in os.walk(target_path)
            for file in files + dirs
            if file in match_list
        ]
        for path in matched_paths:
            remove_target(path)
    except Exception as e:
        raise Exception(f"An error occurred while removing matched targets. Error message: {e}")

    return matched_paths


if __name__ == "__main__":
    try:
        target_path = r'./resources'
        match_list = ['test.txt', 'a']
        removed_paths = remove_target_matched(target_path=target_path, match_list=match_list)
        print("The following files/directories have been removed: ", removed_paths)
    except Exception as e:
        print(e)
