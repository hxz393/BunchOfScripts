import re
import shutil
import logging
from unidecode import unidecode
from pathlib import Path
from typing import Dict, Optional, Union

logger = logging.getLogger(__name__)

EXCLUDE_CHARS = {'æ', 'Æ', 'џ', '®'}
MODIFY_RULES = [
    (r'^the\s|\s\s|, the$', ' '),
    (r'[`∶∗]', '-'),
    (r'？', '')
]


def custom_unidecode(input_string: str) -> str:
    """
    自定义的字符串转换函数，将特殊字符转换为ASCII字符，但排除一些特定字符。

    :type input_string: str
    :param input_string: 需要转换的原始字符串。
    :rtype: str
    :return: 返回处理过后的字符串。
    """
    return ''.join(char if char in EXCLUDE_CHARS else unidecode(char) for char in input_string)


def rename_folder_to_common(source_path: Union[str, Path], target_path: Union[str, Path]) -> Optional[Dict[str, str]]:
    """
    将源目录下的文件夹按照预设规则重命名并移动到目标目录。

    :param source_path: 需要整理的源目录。
    :param target_path: 用于存放重命名后文件夹的目标目录。
    :return: 包含原文件夹路径和重命名后的文件夹路径的字典。如果出现错误，返回 None。
    """
    source = Path(source_path)
    target = Path(target_path)

    if not source.exists() or not source.is_dir():
        logger.error(f"Source directory: '{source}' does not exist or is not a valid directory path.")
        return None

    if not target.exists() or not target.is_dir():
        logger.error(f"Target directory: '{target}' does not exist or is not a valid directory path.")
        return None

    final_path_dict = {}

    folders = [f for f in source.iterdir() if f.is_dir() and ord(f.name[0]) <= 0x0250]
    for folder in folders:
        new_folder_name = folder.name
        for rule, replacement in MODIFY_RULES:
            new_folder_name = re.sub(rule, replacement, new_folder_name, flags=re.IGNORECASE)
        new_folder_name = custom_unidecode(new_folder_name).strip()

        if folder.name != new_folder_name:
            final_path = target / new_folder_name
            if final_path.exists():
                logger.error(f"Folder with the same name already exists in the target directory '{new_folder_name}'.")
                continue
            try:
                shutil.move(str(folder), str(final_path))
                final_path_dict[str(folder)] = str(final_path)
            except OSError as e:
                logger.error(f"Error moving directory: {e}")

    return final_path_dict if final_path_dict else None
