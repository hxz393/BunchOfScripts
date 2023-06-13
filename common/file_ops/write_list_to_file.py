import os
from typing import List

def write_list_to_file(path: str, content: List[str]) -> None:
    """
    将列表的元素写入文件，每个元素占据文件的一行。

    :param path: 文本文件的路径
    :param content: 要写入的列表
    :return: None
    """
    try:
        # 创建文件夹
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, 'w', encoding="utf-8") as file:
            # 使用换行符连接列表的元素，并写入文件
            file.write("\n".join(str(element) for element in content))

    except IOError as e:
        print(f"写入文件时发生错误：{e}")

    except Exception as e:
        print(f"发生了意外的错误：{e}")


if __name__ == '__main__':
    写入列表 = [1, 'a', '啊']
    文本文件路径 = r'resources\new.txt'
    write_list_to_file(path=文本文件路径, content=写入列表)