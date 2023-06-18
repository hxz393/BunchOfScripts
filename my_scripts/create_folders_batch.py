from pathlib import Path
import logging
from typing import Union, Optional, List

from my_module import read_file_to_list
from my_module import sanitize_filename

logger = logging.getLogger(__name__)


def create_folders_batch(target_path: Union[str, Path], txt_file: Union[str, Path]) -> Optional[List[str]]:
    """
    批量创建文件夹的函数。

    :param txt_file: 包含文件夹名称的文件路径
    :type txt_file: Union[str, Path]
    :param target_path: 目标目录
    :type target_path: Union[str, Path]
    :return: 创建的文件夹名称列表，如果过程中有错误发生，返回 None。
    :rtype: Optional[List[str]]
    """
    target_directory_path = Path(target_path)
    MAX_PATH_LENGTH = 260

    if not target_directory_path.exists():
        logger.error(f"The target directory {target_path} does not exist.")
        return None

    name_list = read_file_to_list(txt_file)

    if not name_list:
        logger.error(f"The list of folder names read from the file {txt_file} is empty.")
        return None

    name_list = [sanitize_filename(i) for i in name_list]
    successfully_created = []

    for name in name_list:
        folder_path = target_directory_path / name
        if len(str(folder_path)) > MAX_PATH_LENGTH:
            logger.error(f"The length of the folder path {folder_path} exceeds the maximum supported length in Windows.")
            continue
        try:
            folder_path.mkdir(parents=True, exist_ok=True)
            successfully_created.append(name)
        except Exception as e:
            logger.error(f"An error occurred while creating the folder {folder_path}. Error message: {str(e)}")

    return successfully_created if successfully_created else None
