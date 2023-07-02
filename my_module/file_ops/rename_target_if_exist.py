"""
这是一个Python文件，其中包含一个函数：`rename_target_if_exist`。

函数 `rename_target_if_exist` 用于检查路径是否存在，如果存在，则将其重命名。它接受一个参数 `path`，这是需要重命名的路径。这个路径可以是字符串或 `pathlib.Path` 对象。函数首先检查 `path` 的类型，如果它不是 `pathlib.Path` 对象，就将其转化为 `pathlib.Path` 对象。然后，函数会检查 `path` 是否为空或无效。如果 `path` 存在，那么它会创建一个新的 `path`，在其原始名称后添加一个计数器，并且每次迭代计数器都会增加。如果所有操作都成功，函数将返回新的 `path` 的字符串表示，否则返回 `None`。

这个模块主要用于文件系统操作，包括检查路径是否存在，并在需要时重命名路径。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import logging
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)


def rename_target_if_exist(path: Union[str, Path]) -> Union[str, None]:
    """
    如果目标路径存在，则重命名。

    :param path: 需要重命名的路径
    :type path: Union[str, Path]
    :return: 重命名后的路径，如果遇到错误则返回 None。
    :rtype: Union[str, None]
    """
    try:
        if not isinstance(path, Path):
            path = Path(path)

        if path is None or str(path).strip() == '':
            logger.error("The path is empty or invalid.")
            return None

        original_path = path
        counter = 1
        while path.exists():
            path = original_path.with_stem(f"{original_path.stem}_({counter})")
            counter += 1

        return str(path)
    except Exception as e:
        logger.error(f"An error occurred while renaming the target: {e}")
        return None
