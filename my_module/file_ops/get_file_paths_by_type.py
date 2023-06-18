import os
from typing import List, Union, Optional
import logging

logger = logging.getLogger(__name__)

def get_file_paths_by_type(target_path: Union[str, os.PathLike], type_list: List[str]) -> Optional[List[str]]:
    """
    获取指定路径下特定类型文件的路径列表。

    :type target_path: Union[str, os.PathLike]
    :param target_path: 需要搜索的路径
    :type type_list: List[str]
    :param type_list: 拿来匹配的文件类型列表
    :rtype: Optional[List[str]]
    :return: 匹配的文件路径列表，如果发生错误则返回 None。
    """
    try:
        if not os.path.exists(target_path):
            logger.error(f"The path '{target_path}' does not exist.")
            return None
        if not os.path.isdir(target_path):
            logger.error(f"'{target_path}' is not a valid directory.")
            return None
        if not type_list:
            logger.error(f"The file type list is empty.")
            return None

        type_list = [file_type.lower() for file_type in type_list]
        return [os.path.normpath(os.path.join(root, file)) for root, _, files in os.walk(target_path) for file in files if os.path.splitext(file)[1].lower() in type_list]
    except Exception as e:
        logger.error(f"An error occurred while retrieving file paths: {e}")
        return None
