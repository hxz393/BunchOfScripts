from pathlib import Path
from typing import Iterator, Union


def get_folder_paths(path: Union[Path, str]) -> Iterator[str]:
    """
    获取目标目录下扫描到的所有文件夹路径生成器

    :param path: 目标目录
    :return: 文件夹路径生成器
    :raise ValueError: 如果路径不存在或者不是一个有效的目录，抛出 ValueError
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception
    """
    if isinstance(path, str):
        path = Path(path)

    if not path.exists():
        raise ValueError(f"路径 '{path}' 不存在")

    if not path.is_dir():
        raise ValueError(f"'{path}' 不是一个有效的目录")

    try:
        for entry in path.iterdir():
            if entry.is_dir():
                yield str(entry)
                yield from get_folder_paths(entry)
    except Exception as e:
        raise Exception(f"在获取文件夹路径时发生错误: {e}")


if __name__ == '__main__':
    目标目录 = Path('resources')
    try:
        文件夹路径生成器 = get_folder_paths(path=目标目录)
        for 文件夹路径 in 文件夹路径生成器:
            print(文件夹路径)
    except Exception as e:
        print(e)
