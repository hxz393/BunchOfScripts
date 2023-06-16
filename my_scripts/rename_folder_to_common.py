import re
from unidecode import unidecode
from pathlib import Path

EXCLUDE_CHARS = {'æ', 'Æ', 'џ', '®'}
MODIFY_RULES = [
    (r'^the\s|\s\s|, the$', ' '),
    (r'[`∶∗]', '-'),
    (r'？', '')
]

def custom_unidecode(input_string: str) -> str:
    """
    自定义的字符串转换函数，将特殊字符转换为ASCII字符，但排除一些特定字符。排除不以ASCII字符开头的文件名。

    :param input_string: 需要转换的原始字符串。
    :return: 返回处理过后的字符串。
    """
    return ''.join(char if char in EXCLUDE_CHARS else unidecode(char) for char in input_string)

def rename_folder_to_common(source_dir: str, target_dir: str) -> None:
    """
    将源目录下的文件夹按照预设规则重命名并移动到目标目录。

    :param source_dir: 需要整理的源目录。
    :param target_dir: 用于存放重命名后文件夹的目标目录。
    :raise ValueError: 如果源目录或目标目录不存在，或者不是有效的目录路径，将抛出异常。
    :raise Exception: 如果移动文件夹时出现问题，将抛出异常。
    """

    source_dir = Path(source_dir)
    target_dir = Path(target_dir)

    if not source_dir.exists() or not source_dir.is_dir():
        raise ValueError("源目录不存在或不是有效的目录路径。")

    if not target_dir.exists() or not target_dir.is_dir():
        raise ValueError("目标目录不存在或不是有效的目录路径。")

    for folder in source_dir.iterdir():
        if folder.is_dir() and ord(folder.name[0]) <= 0x0250:
            new_folder_name = folder.name
            for rule, replacement in MODIFY_RULES:
                new_folder_name = re.sub(rule, replacement, new_folder_name, flags=re.IGNORECASE)
            new_folder_name = custom_unidecode(new_folder_name).strip()

            if folder.name != new_folder_name:
                final_path = target_dir / new_folder_name
                if final_path.exists():
                    raise Exception(f"目标目录下已存在同名文件夹 {new_folder_name}。")
                else:
                    folder.rename(final_path)
                    print(f"移动 {folder.name} 到 {final_path} 完成")


if __name__ == "__main__":
    try:
        rename_folder_to_common("resources/1", "resources/2")
    except Exception as e:
        print(str(e))
