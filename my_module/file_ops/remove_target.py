from pathlib import Path
import shutil
import os, stat
from typing import Callable, Any, Union, NoReturn


def remove_permissions(func: Callable[[Path], Any], path: Path, _: Any) -> None:
    """
    移除目标路径的权限并调用指定的函数。

    :param func: 要调用的函数。
    :type func: Callable[[Path], Any]
    :param path: 目标路径。
    :type path: Path
    :param _: 用于异常处理的错误信息。
    :type _: Any
    """
    path.chmod(0o777)
    os.chmod(path, stat.S_IWRITE)
    func(path)


# noinspection PyShadowingNames
def remove_target(path: Union[str, Path]) -> NoReturn:
    """
    删除指定文件或目录。

    :param path: 要删除的文件或目录的路径。
    :type path: Union[str, Path]
    :raise FileNotFoundError: 如果路径不存在，抛出 FileNotFoundError。
    :raise ValueError: 如果路径不是一个有效的文件或目录，抛出 ValueError。
    :raise PermissionError: 如果权限不足，抛出 PermissionError。
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception。
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Path '{path}' does not exist.")

    try:
        if path.is_dir():
            shutil.rmtree(path, onerror=remove_permissions)
        elif path.is_file():
            path.unlink()
        else:
            raise ValueError(f"'{path}' is neither a file nor a directory.")
    except PermissionError:
        remove_permissions(lambda x: None, path, None)
        path.unlink()
    except Exception as e:
        raise Exception(f"An error occurred while removing path '{path}': {e}.")
