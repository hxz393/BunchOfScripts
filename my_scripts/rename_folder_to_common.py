import re
from unidecode import unidecode
from pathlib import Path
from typing import Dict

EXCLUDE_CHARS = {'æ', 'Æ', 'џ', '®'}
MODIFY_RULES = [
    (r'^the\s|\s\s|, the$', ' '),
    (r'[`∶∗]', '-'),
    (r'？', '')
]


def custom_unidecode(input_string: str) -> str:
    """
    自定义的字符串转换函数，将特殊字符转换为ASCII字符，但排除一些特定字符。排除不以ASCII字符开头的文件名。

    :type input_string: str
    :param input_string: 需要转换的原始字符串。
    :rtype: str
    :return: 返回处理过后的字符串。
    """
    return ''.join(char if char in EXCLUDE_CHARS else unidecode(char) for char in input_string)


def rename_folder_to_common(source_path: str, target_path: str) -> Dict[str, str]:
    """
    将源目录下的文件夹按照预设规则重命名并移动到目标目录。

    :type source_path: str
    :param source_path: 需要整理的源目录。
    :type target_path: str
    :param target_path: 用于存放重命名后文件夹的目标目录。
    :rtype: Dict[str, str]
    :return: 包含原文件夹路径和重命名后的文件夹路径的字典。
    :raise ValueError: 如果源目录或目标目录不存在，或者不是有效的目录路径，将抛出异常。
    :raise Exception: 如果移动文件夹时出现问题，将抛出异常。
    """
    source = Path(source_path)
    target = Path(target_path)

    if not source.exists() or not source.is_dir():
        raise ValueError(f"Source directory: '{source}' does not exist or is not a valid directory path.")

    if not target.exists() or not target.is_dir():
        raise ValueError(f"Target directory: '{target}' does not exist or is not a valid directory path.")

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
                raise Exception(f"Folder with the same name already exists in the target directory '{new_folder_name}'.")

            folder.rename(final_path)
            final_path_dict[str(folder)] = str(final_path)

    return final_path_dict


if __name__ == "__main__":
    source_path = r"resources/1"
    target_path = r"resources/2"
    try:
        final_path_dict = rename_folder_to_common(source_path=source_path, target_path=target_path)
        print(final_path_dict)
    except Exception as e:
        print(str(e))
