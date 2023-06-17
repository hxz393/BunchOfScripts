from pathlib import Path
from typing import Union

from my_comm import read_file_to_list
from my_comm import sanitize_filename


def create_folders_batch(target_path: Union[str, Path], txt_file: Union[str, Path]) -> None:
    """
    批量创建文件夹的函数。

    :param txt_file: 包含文件夹名称的文件路径
    :type txt_file: Union[str, Path]
    :param target_path: 目标目录
    :type target_path: Union[str, Path]
    :raise FileNotFoundError: 如果目标目录或者文件不存在会抛出此异常
    :raise Exception: 如果创建文件夹过程中出现异常会抛出此异常
    """

    target_directory_path = Path(target_path)
    MAX_PATH_LENGTH = 260

    if not target_directory_path.exists():
        raise FileNotFoundError(f"The target directory {target_path} does not exist.")

    name_list = read_file_to_list(txt_file)

    if not name_list:
        raise ValueError(f"The list of folder names read from the file {txt_file} is empty.")

    name_list = [sanitize_filename(i) for i in name_list]

    for name in name_list:
        folder_path = target_directory_path / name
        if len(str(folder_path)) > MAX_PATH_LENGTH:
            raise ValueError(f"The length of the folder path {folder_path} exceeds the maximum supported length in Windows.")
        try:
            folder_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise Exception(f"An error occurred while creating the folder {folder_path}. Error message: {str(e)}")

if __name__ == "__main__":
    try:
        create_folders_batch(
            r"resources/1",
            r"resources/new.txt"
        )
    except Exception as e:
        print(str(e))
