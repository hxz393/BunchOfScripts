from pathlib import Path
from typing import Union

from my_comm import read_file_to_list
from my_comm import sanitize_filename

MAX_PATH_LENGTH = 260

def create_folders_batch(file: Union[str, Path], target_directory: Union[str, Path]) -> None:
    """
    批量创建文件夹的函数。

    :param file: 包含文件夹名称的文件路径
    :type file: Union[str, Path]
    :param target_directory: 目标目录
    :type target_directory: Union[str, Path]
    :raise FileNotFoundError: 如果目标目录或者文件不存在会抛出此异常
    :raise Exception: 如果创建文件夹过程中出现异常会抛出此异常
    """

    target_directory_path = Path(target_directory)

    # 检查目标目录是否存在
    if not target_directory_path.exists():
        raise FileNotFoundError(f"目标目录 {target_directory} 不存在.")

    # 从文件中读取文件夹名称列表
    name_list = read_file_to_list(file)

    # 检查文件夹名称列表是否为空
    if not name_list:
        raise ValueError(f"从文件 {file} 中读取的文件夹名称列表为空.")

    # 清洗文件夹名称，移除非法字符
    name_list = [sanitize_filename(i) for i in name_list]

    # 批量创建文件夹
    for name in name_list:
        folder_path = target_directory_path / name
        # 检查路径长度
        if len(str(folder_path)) > MAX_PATH_LENGTH:
            raise ValueError(f"文件夹路径 {folder_path} 的长度超过了 Windows 的最大支持长度.")
        try:
            folder_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise Exception(f"创建文件夹 {folder_path} 时出错. 错误信息: {str(e)}")

if __name__ == "__main__":
    try:
        create_folders_batch(
            r"resources/new.txt",
            r"resources"
        )
    except Exception as e:
        print(str(e))
