"""
这个Python文件包含一个名为 `get_file_paths` 的函数，它用于获取指定目录下所有文件的路径。

给定一个目标目录的路径，这个函数会尝试遍历目录下的所有文件，为每个文件生成一个完整的文件路径，
然后将这些路径收集到一个列表中并返回。这个功能可以用来为文件系统操作，如文件复制或删除，提供输入。

函数的使用方式如下：

```python
file_paths = get_file_paths("/path/to/directory")
if file_paths:
    for file_path in file_paths:
        print(file_path)
```

如果目标路径不存在，或者它不是一个有效的目录，函数会记录一条错误信息并返回 None。
同样，如果在尝试检索文件路径时发生任何其他错误，函数也会记录一条错误信息并返回 None。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393。保留所有权利。
"""
import logging

import os
from typing import List, Union, Optional

logger = logging.getLogger(__name__)


def get_file_paths(target_path: Union[str, os.PathLike]) -> Optional[List[str]]:
    """
    获取目标目录下所有文件的路径。

    :type target_path: Union[str, os.PathLike]
    :param target_path: 目标目录的路径，可以是字符串或 os.PathLike 对象。
    :rtype: Optional[List[str]]
    :return: 一个列表，包含所有文件的路径，或者在发生错误时返回 None。
    """
    try:
        if not os.path.exists(target_path):
            logger.error(f"The path '{target_path}' does not exist.")
            return None
        if not os.path.isdir(target_path):
            logger.error(f"'{target_path}' is not a valid directory.")
            return None
        return [os.path.normpath(os.path.join(root, file)) for root, _, files in os.walk(target_path) for file in files]
    except Exception:
        logger.exception("An error occurred while retrieving file paths")
        return None
