import os
import magic
from typing import Optional, Union

def get_file_type(target_path: Union[str, os.PathLike]) -> Optional[str]:
    """
    以读取文件头部内容的方式，取得指定文件的真实类型

    :type target_path: Union[str, os.PathLike]
    :param target_path: 要检测的文件路径，可以是字符串或 os.PathLike 对象。
    :rtype: Optional[str]
    :return: 文件类型检测结果，如果检测失败则返回 None。
    :raise FileNotFoundError: 如果文件不存在，抛出 FileNotFoundError。
    :raise PermissionError: 如果文件无法访问，抛出 PermissionError。
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception。
    """
    target_path = os.path.normpath(target_path)
    if not os.path.exists(target_path):
        raise FileNotFoundError(f"The file '{target_path}' does not exist.")
    if not os.path.isfile(target_path):
        raise ValueError(f"'{target_path}' is not a valid file.")

    try:
        with open(target_path, 'rb') as f:
            file_type = magic.from_buffer(f.read(1024), mime=True)
    except PermissionError:
        raise PermissionError(f"Unable to access file '{target_path}', permission denied.")
    except Exception as e:
        raise Exception(f"An error occurred while detecting the file type: {e}")

    return file_type


if __name__ == '__main__':
    target_file = r'resources/new.rar'
    try:
        file_type = get_file_type(target_path=target_file)
        print(file_type)
    except Exception as e:
        print(e)
