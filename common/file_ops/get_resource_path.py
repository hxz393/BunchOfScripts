import os
import sys
from typing import Union


# noinspection PyUnresolvedReferences,PyProtectedMember
def get_resource_path(relative_path: Union[str, os.PathLike]) -> str:
    """
    获取资源的绝对路径。这个函数适用于 PyInstaller 打包后的可执行文件。

    :param relative_path: 相对路径，可以是字符串或 os.PathLike 对象
    :type relative_path: Union[str, os.PathLike]
    :raise TypeError: 如果输入的相对路径类型不是字符串或 os.PathLike。
    :return: 资源的绝对路径
    :rtype: str
    """
    if not isinstance(relative_path, (str, os.PathLike)):
        raise TypeError(f"输入的相对路径 {relative_path} 类型应为字符串或 os.PathLike")

    try:
        # PyInstaller 创建一个临时文件夹，并把路径存储在_MEIPASS
        base_path = sys._MEIPASS
    except AttributeError:
        # 如果不是用 PyInstaller 打包的，那么就是普通的Python脚本，使用当前目录作为基础路径
        base_path = os.path.abspath(".")

    # 将相对路径中的斜线替换为当前系统的路径分隔符
    relative_path = relative_path.replace("/", os.sep)

    return os.path.join(base_path, relative_path)


if __name__ == '__main__':
    # 打包 pyinstaller -F common/file_ops/get_resource_path.py
    # 运行 .\dist\get_resource_path.exe
    try:
        打包命令加入的相对路径 = r"resources/new.txt"
        运行时解压到的临时绝对路径 = get_resource_path(relative_path=打包命令加入的相对路径)
        print(运行时解压到的临时绝对路径)  # 输出： '/tmp/_MEIabc123/resources/new.txt' 或 './resources/new.txt'
    except Exception as e:
        print(e)
