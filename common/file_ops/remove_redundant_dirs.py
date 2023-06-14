from typing import Union
from pathlib import Path, PurePath
import uuid


def remove_redundant_dirs(path: Union[str, Path]) -> None:
    """
    该函数用于消除冗余的目录结构。如果给定目录下只有一个子目录，并且子目录的名称与父目录相同，
    并且父目录中没有其他文件，那么就会删除这个冗余的子目录，并将其下的所有项目移至父目录下。

    :param path: 需要优化的目标路径，可以是字符串或 Path 对象
    :type path: Union[str, Path]
    :raise FileNotFoundError: 如果路径不存在
    :raise NotADirectoryError: 如果路径不是一个目录
    :raise Exception: 如果在处理过程中出现其他错误
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
        raise NotADirectoryError(f"路径 {path} 不是一个目录")

    try:
        # 获取目录下的所有子目录
        subdirs = [subdir for subdir in path.iterdir() if subdir.is_dir()]

        # 如果有且仅有一个子目录，且子目录名与父目录名相同
        if len(subdirs) == 1 and subdirs[0].name == path.name:
            subdir_path = subdirs[0]
            # 检查父目录中是否有其他文件
            parent_files = [file for file in path.iterdir() if file.is_file()]

            # 如果父目录中没有其他文件
            if not parent_files:
                # 创建一个临时目录
                temp_dir = path / f"{subdir_path.name}_{uuid.uuid4()}"
                # 将子目录重命名为临时目录
                subdir_path.rename(temp_dir)

                # 将临时目录中的所有项目移至父目录
                for item in temp_dir.iterdir():
                    item.rename(path / item.name)

                # 如果临时目录为空，则删除
                if not list(temp_dir.iterdir()):
                    temp_dir.rmdir()
    except Exception as e:
        raise Exception(f"处理过程中出现错误：{e}")


if __name__ == '__main__':
    try:
        优化目标路径 = r'resources'
        remove_redundant_dirs(path=优化目标路径)
    except Exception as e:
        print(e)
