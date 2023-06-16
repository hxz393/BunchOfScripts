from typing import Union
from pathlib import Path
from shutil import move

from my_comm.file_ops.rename_target_if_exist import rename_target_if_exist


def move_folder_with_rename(source_path: Union[str, Path], target_path: Union[str, Path]) -> None:
    """
    将文件或文件夹移动到目标位置，如果目标位置已存在相同名称的文件或文件夹，则重命名。

    :param source_path: 源文件或文件夹路径
    :type source_path: Union[str, Path]
    :param target_path: 目标位置路径
    :type target_path: Union[str, Path]
    :raise FileNotFoundError: 如果源文件或文件夹不存在，或者目标位置的父目录不存在，会抛出此异常
    """

    if not isinstance(source_path, Path):
        source_path = Path(source_path)

    if not isinstance(target_path, Path):
        target_path = Path(target_path)

    # 检查源文件或文件夹是否存在
    if not source_path.exists():
        raise FileNotFoundError(f"源文件或文件夹 {source_path} 不存在.")

    # 检查目标位置的父目录是否存在
    if not target_path.parent.exists():
        raise FileNotFoundError(f"目标位置的父目录 {target_path.parent} 不存在.")


    target_path = rename_target_if_exist(target_path)

    try:
        # 尝试移动文件或文件夹
        move(str(source_path), str(target_path))
    except Exception as e:
        raise Exception(f"移动文件或文件夹时出错. 错误信息: {str(e)}")


if __name__ == "__main__":
    try:
        move_folder_with_rename(r'resources/new1/new2', r'resources/new1/new2')
    except Exception as e:
        print(str(e))
