from pathlib import Path
from typing import List

def get_folder_paths(path: str) -> List[str]:
    """
    获取目标目录下的所有文件夹路径列表

    :param path: 目标目录
    :return: 文件夹路径列表
    """
    path = Path(path)
    folder_paths = []

    try:
        if not path.exists() or not path.is_dir():
            raise ValueError(f"目录 '{path}' 不存在或不是一个有效的目录")

        for entry in path.iterdir():
            if entry.is_dir():
                folder_paths.append(str(entry))
                folder_paths.extend(get_folder_paths(str(entry)))

    except Exception as e:
        print(f"获取文件夹路径时发生错误: {e}")

    return folder_paths
