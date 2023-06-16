import os
import json
from typing import Dict, Any, Union


def read_json_to_dict(target_path: Union[str, os.PathLike]) -> Dict[str, Any]:
    """
    读取 JSON 文件内容，储存到字典。

    :type target_path: Union[str, os.PathLike]
    :param target_path: Json 文件的路径，可以是字符串或 os.PathLike 对象。
    :rtype: Dict[str, Any]
    :return: 成功时返回内容字典。
    :raise FileNotFoundError: 如果路径不存在，抛出 FileNotFoundError。
    :raise NotADirectoryError: 如果路径是一个目录，而不是文件，抛出 NotADirectoryError。
    :raise PermissionError: 如果无法访问文件，可能是因为权限错误，抛出 PermissionError。
    :raise ValueError: 如果无法解析 JSON 文件，抛出 ValueError。
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception。
    """
    if not os.path.exists(target_path):
        raise FileNotFoundError(f"The file '{target_path}' does not exist.")
    if not os.path.isfile(target_path):
        raise NotADirectoryError(f"'{target_path}' is not a valid file.")

    try:
        with open(target_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except PermissionError:
        raise PermissionError(f"Cannot access file '{target_path}', permission denied.")
    except json.JSONDecodeError as e:
        raise ValueError(f"Cannot decode JSON file '{target_path}': {e}")
    except Exception as e:
        raise Exception(f"An error occurred while reading the JSON file '{target_path}': {e}")


if __name__ == '__main__':
    try:
        file_path = 'resources/new.json'
        return_dict = read_json_to_dict(target_path=file_path)
        print(return_dict)
    except Exception as e:
        print(f"An error occurred: {e}")
