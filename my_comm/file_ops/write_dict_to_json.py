import json
from pathlib import Path
from typing import Dict, Any, Optional, Union

def write_dict_to_json(target_path: Union[str, Path], data: Dict[str, Any]) -> Optional[bool]:
    """
    将字典数据写入到 JSON 格式文件。

    :type target_path: Union[str, Path]
    :param target_path: Json文件的路径，可以是字符串或 pathlib.Path 对象。
    :type data: Dict[str, Any]
    :param data: 要写入的字典数据。
    :rtype: Optional[bool]
    :return: 成功时返回True，失败时返回None。
    :raise FileNotFoundError: 如果目标路径的父目录不存在，抛出 FileNotFoundError。
    :raise IsADirectoryError: 如果目标路径是一个目录，抛出 IsADirectoryError。
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception。
    """
    try:
        target_path = Path(target_path)

        target_path.parent.mkdir(parents=True, exist_ok=True)

        with target_path.open('w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        return True
    except FileNotFoundError as e:
        raise FileNotFoundError(f"The parent directory of the path '{target_path}' does not exist: {e}")
    except IsADirectoryError as e:
        raise IsADirectoryError(f"'{target_path}' is a directory, not a valid file path: {e}")
    except Exception as e:
        raise Exception(f"An error occurred while writing to the JSON file at '{target_path}': {e}")

if __name__ == '__main__':
    data = {
        "name": "John Doe",
        "age": 30,
        "address": ["New York", 119],
        "pets": {"cat": "meow", "tiger": None}
    }
    target_path = 'resources/new.json'
    try:
        if write_dict_to_json(target_path=target_path, data=data):
            print(f"Writing JSON file: '{target_path}' was successful")
    except Exception as e:
        print(e)
