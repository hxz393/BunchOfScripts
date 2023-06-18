import os
import json
import logging
from typing import Dict, Any, Union, Optional

logger = logging.getLogger(__name__)

def read_json_to_dict(target_path: Union[str, os.PathLike]) -> Optional[Dict[str, Any]]:
    """
    读取 JSON 文件内容，储存到字典。

    :param target_path: Json 文件的路径，可以是字符串或 os.PathLike 对象。
    :type target_path: Union[str, os.PathLike]
    :return: 成功时返回内容字典，如果遇到错误则返回None。
    :rtype: Optional[Dict[str, Any]]
    """
    if not os.path.exists(target_path):
        logger.error(f"The file '{target_path}' does not exist.")
        return None
    if not os.path.isfile(target_path):
        logger.error(f"'{target_path}' is not a valid file.")
        return None

    try:
        with open(target_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except PermissionError:
        logger.error(f"Cannot access file '{target_path}', permission denied.")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Cannot decode JSON file '{target_path}': {e}")
        return None
    except Exception as e:
        logger.error(f"An error occurred while reading the JSON file '{target_path}': {e}")
        return None
