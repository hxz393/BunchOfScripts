from pathlib import Path
from typing import List, Union


def get_subdirectories(path: Union[Path, str]) -> List[str]:
    """
    获取目标目录下的第一级目录路径列表

    :param path: 检测目录，可以是 str 或 Path
    :return: 文件夹路径列表
    :raise ValueError: 如果路径不是一个有效的目录，或者路径不存在，抛出 ValueError
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception
    """
    if isinstance(path, str):
        path = Path(path)

    if not path.exists():
        raise ValueError(f"路径 '{path}' 不存在")

    if not path.is_dir():
        raise ValueError(f"'{path}' 不是一个有效的目录")

    try:
        return [str(item) for item in path.iterdir() if item.is_dir()]
    except Exception as e:
        raise Exception(f"获取文件夹路径时发生错误: {e}")


if __name__ == '__main__':
    目标目录 = Path('resources')
    try:
        返回列表 = get_subdirectories(path=目标目录)
        print(返回列表)
    except Exception as e:
        print(e)
