"""
此 Python 文件包含两个主要函数：`custom_unidecode` 和 `rename_folder_to_common`。这两个函数用于操作文件夹名称，将其标准化并移动到新的目录中。

`custom_unidecode` 是一个自定义的 Unidecode 函数，将输入字符串中的特殊字符转换为 ASCII 字符，但会排除一些特定字符。

`rename_folder_to_common` 函数主要用于处理指定源目录下的文件夹名称，使其更加规范化，然后将处理后的文件夹移动到目标目录中。

函数首先会检查源目录和目标目录的有效性，如果目录无效，则记录错误日志并返回 None。然后，函数会遍历源目录中的所有文件夹，并使用预设规则（`MODIFY_RULES`）对其进行重命名。

重命名后的文件夹将移动到目标目录。如果目标目录中已存在相同名称的文件夹，或在移动过程中发生任何错误，函数将记录错误日志并继续处理下一个文件夹。

最后，函数返回一个字典，其中键是原始文件夹的路径，值是重命名后的新文件夹路径。如果在执行过程中发生任何错误或异常，函数将记录错误日志并返回 None。


:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393。保留所有权利。
"""
import logging
import traceback
import re
import shutil
from pathlib import Path
from typing import Dict, Optional, Union

from unidecode import unidecode

logger = logging.getLogger(__name__)

EXCLUDE_CHARS = {'æ', 'Æ', '®', 'º', 'ß', 'Þ', '·', '±', '°', '©', '¡', '▲', 'Ξ', '™', 'Ɔ', 'Σ', '²', '∞'}
MODIFY_RULES = [
    (r'^the ', ' '),
    (r', the$', ' '),
    (r'\s\s', ' '),
    (r'`', '-'),
    (r'∶', '-'),
    (r'∗', '-'),
    (r'⁄', '-'),
    (r'│', '-'),
    (r'∣', '-'),
    (r'˃', '_'),
    (r'“', ''),
    (r'”', ''),
    (r'·', '-'),
    (r'•', ' ')
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
        logger.error(f"源目录不正确：{source}")
        return None

    if not target.exists() or not target.is_dir():
        logger.error(f"目标目录不正确：{target}")
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
                logger.error(f"移不动 {folder.name}，目标已存在：{new_folder_name}")
                continue
            try:
                shutil.move(str(folder), str(final_path))
                final_path_dict[str(folder)] = str(final_path)
                logger.info(f"{folder} 移动到 {final_path}")
            except OSError as e:
                logger.error(f"移动时出错：{new_folder_name}，错误信息：{e}\n{traceback.format_exc()}")

    return final_path_dict if final_path_dict else None
