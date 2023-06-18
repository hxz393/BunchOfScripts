import logging
from typing import Union, Optional
from pathlib import Path
from shutil import move

from my_module.file_ops.rename_target_if_exist import rename_target_if_exist

logger = logging.getLogger(__name__)

def move_folder_with_rename(source_path: Union[str, Path], target_path: Union[str, Path]) -> Optional[Union[str, Path]]:
    """
    将文件或文件夹移动到目标位置，如果目标位置已存在相同名称的文件或文件夹，则重命名。

    :param source_path: 源文件或文件夹路径
    :type source_path: Union[str, Path]
    :param target_path: 想要移到的目标位置路径
    :type target_path: Union[str, Path]
    :return: 移动并可能被重命名后的目标文件或文件夹的路径，如果发生错误则返回None
    :rtype: Optional[Union[str, Path]]
    """
    source_path = Path(source_path)
    target_path = Path(target_path)

    if not source_path.exists():
        logger.error(f"The source file or folder '{source_path}' does not exist.")
        return None

    if not target_path.parent.exists():
        logger.error(f"The parent directory of the target location '{target_path.parent}' does not exist.")
        return None

    target_path = rename_target_if_exist(target_path)

    try:
        move(str(source_path), str(target_path))
    except Exception as e:
        logger.error(f"An error occurred while moving the file or folder. Error message: {str(e)}")
        return None

    return target_path
