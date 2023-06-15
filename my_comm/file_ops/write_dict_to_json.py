import json
from pathlib import Path
from typing import Dict, Any, Optional

def write_dict_to_json(path: str, data: Dict[str, Any]) -> Optional[bool]:
    """
    将字典数据写入到 JSON 格式文件

    :param path: Json文件的路径
    :param data: 要写入的字典数据
    :return: 成功时返回True，失败时返回None
    :raise ValueError: 如果路径不存在或者不是一个有效的文件，抛出 ValueError
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception
    """
    try:
        # 创建文件夹
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        with open(path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        raise Exception(f"写入JSON文件 '{path}' 时发生错误: {e}")


if __name__ == '__main__':
    try:
        写入字典 = {
            "name": "John Doe",
            "age": 30,
            "address": ["New York", 119],
            "pets": {"cat": "meow", "tiger": None}
        }
        文件路径 = 'resources/new.json'
        if write_dict_to_json(path=文件路径, data=写入字典):
            print("写入成功")
    except Exception as e:
        print(e)
