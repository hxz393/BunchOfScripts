from pathlib import Path
from typing import List, Any, Optional

def write_list_to_file(path: str, content: List[Any]) -> Optional[bool]:
    """
    将列表的元素写入文件，每个元素占据文件的一行。

    :param path: 文本文件的路径
    :param content: 要写入的列表
    :return: 成功时返回True，失败时返回None
    :raise ValueError: 如果路径不存在或者不是一个有效的文件，抛出 ValueError
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception
    """
    try:
        # 创建文件夹
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        with open(path, 'w', encoding="utf-8") as file:
            # 使用换行符连接列表的元素，并写入文件
            file.write("\n".join(str(element) for element in content))

        return True
    except Exception as e:
        raise Exception(f"写入文件 '{path}' 时发生错误: {e}")

    return None


if __name__ == '__main__':
    try:
        写入列表 = [1, 'a', '啊']
        文本文件路径 = 'resources/new.txt'
        if write_list_to_file(path=文本文件路径, content=写入列表):
            print("写入成功")
    except Exception as e:
        print(e)
