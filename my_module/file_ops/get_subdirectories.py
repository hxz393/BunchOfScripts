import os
import logging
from typing import List, Union, Optional

logger = logging.getLogger(__name__)

def get_subdirectories(target_path: Union[os.PathLike, str]) -> Optional[List[str]]:
    """
    获取目标目录下的第一级目录路径列表。

    :param target_path: 检测目录，可以是 str 或 os.PathLike 对象。
    :type target_path: Union[os.PathLike, str]
    :return: 文件夹路径列表或者None。
    :rtype: Optional[List[str]]
    """
    try:
        if not os.path.exists(target_path):
            logger.error(f"The path '{target_path}' does not exist.")
            return None
        if not os.path.isdir(target_path):
            logger.error(f"'{target_path}' is not a valid directory.")
            return None
        return [entry.path for entry in os.scandir(target_path) if entry.is_dir()]
    except Exception as e:
        logger.error(f"An error occurred while getting subdirectories: {e}")
        return None
