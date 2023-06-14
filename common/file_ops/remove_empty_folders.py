from pathlib import Path
from typing import Union
from os import scandir


def remove_empty_folders(path: Union[str, Path]) -> None:
    """
    删除指定路径下的所有空目录。

    :param path: 需要处理的目录路径，可以是字符串或 Path 对象。
    :type path: Union[str, Path]
    :raise FileNotFoundError: 如果路径不存在。
    :raise NotADirectoryError: 如果路径不是一个目录。
    :raise Exception: 如果在处理过程中出现其他错误。
    :return: 无返回值
    """
    # 如果输入的路径是字符串，转化为Path对象
    if isinstance(path, str):
        path = Path(path)

    # 检查路径是否存在
    if not path.exists():
        raise FileNotFoundError(f"路径 {path} 不存在")

    # 检查路径是否是目录
    if not path.is_dir():
        raise NotADirectoryError(f"{path} 不是一个有效的目录")

    try:
        # 使用 os.scandir 替代 pathlib 的 iterdir，性能更优
        for entry in scandir(path):
            if entry.is_dir(follow_symlinks=False):
                # 递归处理子目录
                remove_empty_folders(entry.path)
        if not any(scandir(path)):
            # 删除空目录
            path.rmdir()
    except OSError as e:
        raise Exception(f"无法删除目录 {path}: {str(e)}")


if __name__ == '__main__':
    try:
        目标目录 = r'resources'
        remove_empty_folders(path=目标目录)
    except Exception as e:
        print(e)
