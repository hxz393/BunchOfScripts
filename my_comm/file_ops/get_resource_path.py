import os
import sys
from typing import Union

def get_resource_path(relative_path: Union[str, os.PathLike]) -> str:
    """
    获取资源的绝对路径。这个函数适用于 PyInstaller 打包后的可执行文件。\n
    测试打包：pyinstaller -F my_comm/file_ops/get_resource_path.py\n
    测试运行：.\dist\get_resource_path.exe\n


    :type relative_path: Union[str, os.PathLike]
    :param relative_path: 相对路径，可以是字符串或 os.PathLike 对象。
    :rtype: str
    :return: 资源的绝对路径。
    :raise TypeError: 如果输入的相对路径类型不是字符串或 os.PathLike，抛出 TypeError。
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception。
    """
    if not isinstance(relative_path, (str, os.PathLike)):
        raise TypeError(f"The input relative path '{relative_path}' should be of type str or os.PathLike.")

    try:
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.abspath(".")

    try:
        relative_path = os.path.normpath(relative_path)
        return os.path.join(base_path, relative_path)
    except Exception as e:
        raise Exception(f"An error occurred while retrieving resource path: {e}")

if __name__ == '__main__':
    try:
        packed_relative_path = r'resources/new.txt'
        runtime_absolute_path = get_resource_path(relative_path=packed_relative_path)
        print(runtime_absolute_path)
    except Exception as e:
        print(f"An error occurred: {e}")
