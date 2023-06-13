import json
from typing import Dict, Any


def read_json_to_dict(path: str) -> Dict[str, Any]:
    """
    读取 JSON 文件内容，储存到字典

    :param path: Json 文件的路径
    :return: 返回内容字典
    """
    lang_dict: Dict[str, Any] = {}

    try:
        with open(path, 'r', encoding='utf-8') as file:
            lang_dict = json.load(file)

    except FileNotFoundError:
        print(f"文件 '{path}' 不存在")

    except PermissionError:
        print(f"无法访问文件 '{path}'，权限错误")

    except json.JSONDecodeError as e:
        print(f"无法解析 JSON 文件 '{path}': {e}")

    except Exception as e:
        print(f"读取 JSON 文件 '{path}' 时发生错误: {e}")

    return lang_dict


if __name__ == '__main__':
    文件路径 = r'resources\new.json'
    返回列表 = read_json_to_dict(path=文件路径)
    print(返回列表)