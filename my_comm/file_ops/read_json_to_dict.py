import json
from typing import Dict, Any, Optional


def read_json_to_dict(path: str) -> Optional[Dict[str, Any]]:
    """
    读取 JSON 文件内容，储存到字典

    :param path: Json 文件的路径
    :return: 成功时返回内容字典，失败时返回 None
    :raise ValueError: 如果路径不存在或者不是一个有效的文件，抛出 ValueError
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception
    """
    try:
        with open(path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        raise ValueError(f"文件 '{path}' 不存在")
    except PermissionError:
        raise ValueError(f"无法访问文件 '{path}'，权限错误")
    except json.JSONDecodeError as e:
        raise ValueError(f"无法解析 JSON 文件 '{path}': {e}")
    except Exception as e:
        raise Exception(f"读取 JSON 文件 '{path}' 时发生错误: {e}")


if __name__ == '__main__':
    try:
        文件路径 = 'resources/new.json'
        返回字典 = read_json_to_dict(path=文件路径)
        print(返回字典)
    except Exception as e:
        print(e)
