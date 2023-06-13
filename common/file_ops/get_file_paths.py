from pathlib import Path
from typing import List

def get_file_paths(path: str) -> List[str]:
    """
    获取目标目录下扫描到的所有文件路径列表

    :param path: 目标目录
    :return: 文件路径列表
    """
    path = Path(path)
    file_paths = []

    try:
        if not path.exists() or not path.is_dir():
            raise ValueError(f"目录 '{path}' 不存在或不是一个有效的目录")

        file_paths = [str(file_path) for file_path in path.rglob('*') if file_path.is_file()]

    except Exception as e:
        print(f"获取文件路径时发生错误: {e}")

    return file_paths


if __name__ == '__main__':
    目标目录 = r'resources'
    返回列表 = get_file_paths(path=目标目录)
    print(返回列表)