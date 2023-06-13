from pathlib import Path
from typing import List

def get_subdirectories(path: str) -> List[str]:
    """
    获取目标目录下的第一级文件夹路径列表

    :param path: 检测目录
    :return: 文件夹路径列表
    """
    subdirectories = []
    path = Path(path)

    try:
        if not path.exists() or not path.is_dir():
            raise ValueError(f"目录 '{path}' 不存在或不是一个有效的目录")

        subdirectories = [str(item) for item in path.iterdir() if item.is_dir()]

    except Exception as e:
        print(f"获取文件夹路径时发生错误: {e}")

    return subdirectories
