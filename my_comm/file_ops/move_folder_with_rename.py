from typing import Union
from pathlib import Path
from shutil import move

from my_comm.file_ops.rename_target_if_exist import rename_target_if_exist


def move_folder_with_rename(source: Union[str, Path], destination: Union[str, Path]) -> None:
    """
    将文件或文件夹移动到目标位置，如果目标位置已存在相同名称的文件或文件夹，则重命名。

    :param source: 源文件或文件夹路径
    :type source: Union[str, Path]
    :param destination: 目标位置路径
    :type destination: Union[str, Path]
    :raise FileNotFoundError: 如果源文件或文件夹不存在，或者目标位置的父目录不存在，会抛出此异常
    """

    if not isinstance(source, Path):
        source = Path(source)

    if not isinstance(destination, Path):
        destination = Path(destination)

    # 检查源文件或文件夹是否存在
    if not source.exists():
        raise FileNotFoundError(f"源文件或文件夹 {source} 不存在.")

    # 检查目标位置的父目录是否存在
    if not destination.parent.exists():
        raise FileNotFoundError(f"目标位置的父目录 {destination.parent} 不存在.")


    destination = rename_target_if_exist(destination)

    try:
        # 尝试移动文件或文件夹
        move(str(source), str(destination))
    except Exception as e:
        raise Exception(f"移动文件或文件夹时出错. 错误信息: {str(e)}")


if __name__ == "__main__":
    try:
        move_folder_with_rename(r'resources/new1/new2', r'resources/new1/new2')
    except Exception as e:
        print(str(e))
