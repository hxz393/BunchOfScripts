import os
import json

def write_dict_to_json(data: dict, path: str) -> None:
    """
    将字典数据写入到 JSON 格式文件

    :param data: 要写入的字典数据
    :param path: JSON文件的路径
    :return: None
    """
    try:
        # 创建文件夹
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"写入JSON文件 '{path}' 时发生错误: {e}")


if __name__ == '__main__':
    写入字典 = {
        "name": "John Doe",
        "age": 30,
        "address": ["New York", 119],
        "pets": {"cat": "meow", "tiger": None}
    }
    文件路径 = r'resources\new.json'
    write_dict_to_json(data=写入字典, path=文件路径)
