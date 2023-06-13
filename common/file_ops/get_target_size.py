from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Union


def get_file_size(path: Path) -> int:
    """
    直接返回文件大小

    :param path: 文件路径，Path 对象
    :return: 文件大小（字节数）
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception
    """
    try:
        return path.stat().st_size
    except Exception as e:
        raise Exception(f"获取文件大小时发生错误: {e}")


def get_target_size(path: Union[str, Path]) -> Optional[int]:
    """
    获取目标文件或文件夹的大小

    :param path: 文件或文件夹的路径，可以是 str 或 Path 对象
    :return: 文件或文件夹的大小（字节数），如果检测失败返回 None
    :raise ValueError: 如果路径不存在或者不是一个有效的文件/文件夹，抛出 ValueError
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception
    """
    if isinstance(path, str):
        path = Path(path)

    if not path.exists():
        raise ValueError(f"路径 '{path}' 不存在")

    try:
        if path.is_file():
            return path.stat().st_size
        elif path.is_dir():
            with ThreadPoolExecutor() as executor:
                files = list(path.rglob('*'))
                sizes = executor.map(get_file_size, files)
                return sum(sizes)
        else:
            raise ValueError(f"'{path}' 不是一个有效的文件或文件夹")
    except Exception as e:
        raise Exception(f"获取文件或文件夹大小时发生错误: {e}")


if __name__ == '__main__':
    try:
        目标文件 = Path('resources/new.json')
        文件大小 = get_target_size(path=目标文件)
        print(文件大小)

        目标目录 = Path('resources')
        目录大小 = get_target_size(path=目标目录)
        print(目录大小)
    except Exception as e:
        print(e)
