from pathlib import Path
from typing import List, Any, Optional, Union


def write_list_to_file(target_path: Union[str, Path], content: List[Any]) -> Optional[bool]:
    """
    将列表的元素写入文件，每个元素占据文件的一行。

    :type target_path: Union[str, Path]
    :param target_path: 文本文件的路径，可以是字符串或 pathlib.Path 对象。
    :type content: List[Any]
    :param content: 要写入的列表。
    :rtype: Optional[bool]
    :return: 成功时返回True，失败时返回None。
    :raise IsADirectoryError: 如果路径是一个目录，抛出 IsADirectoryError。
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception。
    """
    try:
        target_path = Path(target_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)

        with target_path.open('w', encoding="utf-8") as file:
            file.write("\n".join(str(element) for element in content))
        return True
    except PermissionError as e:
        raise PermissionError(f"'{target_path}' is a directory, not a valid file path: {e}")
    except Exception as e:
        raise Exception(f"An error occurred while writing to the file at '{target_path}': {e}")
