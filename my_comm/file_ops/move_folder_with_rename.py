from typing import Union
from pathlib import Path
from shutil import move

from my_comm.file_ops.rename_target_if_exist import rename_target_if_exist


def move_folder_with_rename(source_path: Union[str, Path], target_path: Union[str, Path]) -> Union[str, Path]:
    """
    将文件或文件夹移动到目标位置，如果目标位置已存在相同名称的文件或文件夹，则重命名。

    :param source_path: 源文件或文件夹路径
    :type source_path: Union[str, Path]
    :param target_path: 想要移到的目标位置路径
    :type target_path: Union[str, Path]
    :rtype: Union[str, Path]
    :return: 移动并可能被重命名后的目标文件或文件夹的路径
    :raise FileNotFoundError: 如果源文件或文件夹不存在，或者目标位置的父目录不存在，会抛出此异常
    :raise Exception: 如果在移动文件或文件夹过程中发生错误，会抛出此异常
    """
    source_path = Path(source_path)
    target_path = Path(target_path)

    if not source_path.exists():
        raise FileNotFoundError(f"The source file or folder '{source_path}' does not exist.")

    if not target_path.parent.exists():
        raise FileNotFoundError(f"The parent directory of the target location '{target_path.parent}' does not exist.")

    target_path = rename_target_if_exist(target_path)

    try:
        move(str(source_path), str(target_path))
    except Exception as e:
        raise Exception(f"An error occurred while moving the file or folder. Error message: {str(e)}")

    return target_path


if __name__ == "__main__":
    try:
        source_path = r'resources/new1/new2'
        target_path = r'resources/new2'
        moved_path = move_folder_with_rename(source_path=source_path, target_path=target_path)
        print(f"The file or folder: '{Path(source_path)}' has been moved and possibly renamed to: '{moved_path}'")
    except Exception as e:
        print(str(e))
