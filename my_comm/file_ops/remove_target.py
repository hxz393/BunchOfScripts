from pathlib import Path
import shutil
import os, stat
from typing import Callable, Any, Union


def remove_permissions(func: Callable[[Path], Any], path: Path, _: Any) -> None:
    """
    去除目标路径的权限并调用指定的函数

    :param func: 要调用的函数
    :param path: 目标路径
    :param _ : 用于异常处理的错误信息
    """
    path.chmod(0o777)
    os.chmod(path, stat.S_IWRITE)
    func(path)


def remove_target(path: Union[str, Path]) -> None:
    """
    删除指定文件或目录

    :param path: 要删除的文件或目录的路径
    :raise FileNotFoundError: 如果路径不存在，抛出 FileNotFoundError
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"路径 '{path}' 不存在")

    try:
        if path.is_dir():
            shutil.rmtree(str(path), onerror=remove_permissions)
        elif path.is_file():
            path.unlink()
        else:
            raise ValueError(f"'{path}' 既不是文件也不是目录")
    except PermissionError:
        remove_permissions(lambda x: None, path, None)
        path.unlink()
    except Exception as e:
        raise Exception(f"在删除路径 '{path}' 时发生错误: {e}")


if __name__ == '__main__':
    try:
        目标删除文件 = r'1/1.txt'
        目标删除目录 = r'1'
        remove_target(path=目标删除文件)
        remove_target(path=目标删除目录)
    except Exception as e:
        print(e)

