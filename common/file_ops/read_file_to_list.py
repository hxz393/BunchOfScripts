from typing import List

def read_file_to_list(path: str) -> List[str]:
    """
    读取文本文件中的内容，并将其存储成列表。

    :param path: 文本文件的路径
    :return: 成功时返回文本内容列表，失败时返回空列表
    """
    content = []

    try:
        with open(path, 'r', encoding="utf-8") as file:
            content = [line.strip() for line in file]

    except FileNotFoundError:
        print(f"文件 '{path}' 不存在")

    except PermissionError:
        print(f"无法访问文件 '{path}'，权限错误")

    except IsADirectoryError:
        print(f"路径 '{path}' 是一个目录")

    except UnicodeDecodeError:
        print(f"无法解码文件 '{path}'，请转换文件编码为 'UTF-8' 格式")

    except Exception as e:
        print(f"读取文件 '{path}' 时出现错误: {e}")

    return content
