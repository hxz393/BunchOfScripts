from typing import List, Optional


def read_file_to_list(path: str) -> Optional[List[str]]:
    """
    读取文本文件中的内容，并将其存储成列表。

    :param path: 文本文件的路径
    :return: 成功时返回文本内容列表，失败时返回 None
    :raise ValueError: 如果路径不存在或者不是一个有效的文件，抛出 ValueError
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception
    """

    try:
        with open(path, 'r', encoding="utf-8") as file:
            content = [line.strip() for line in file]
    except FileNotFoundError:
        raise ValueError(f"文件 '{path}' 不存在")
    except IsADirectoryError:
        raise ValueError(f"路径 '{path}' 是一个目录")
    except PermissionError:
        raise ValueError(f"无法访问文件 '{path}'，权限错误")
    except UnicodeDecodeError:
        raise ValueError(f"无法解码文件 '{path}'，请检查是否为 'UTF-8' 格式")
    except Exception as e:
        raise Exception(f"读取文件 '{path}' 时出现错误: {e}")

    return content


if __name__ == '__main__':
    try:
        文本文件路径 = 'resources/new.txt'
        返回列表 = read_file_to_list(path=文本文件路径)
        print(返回列表)
    except Exception as e:
        print(e)
