"""
这个Python文件包含一个名为 `get_file_paths_by_type` 的函数，它用于获取指定目录下特定类型文件的路径列表。

给定一个目标目录的路径和一个文件类型列表，这个函数会尝试遍历目录下的所有文件，匹配类型列表中的文件，
为每个匹配的文件生成一个完整的文件路径，然后将这些路径收集到一个列表中并返回。这个功能可以用来为文件系统操作，如文件复制或删除，提供输入。

函数的使用方式如下：

```python
file_paths = get_file_paths_by_type("/path/to/directory", [".txt", ".docx"])
if file_paths:
    for file_path in file_paths:
        print(file_path)
```

如果目标路径不存在，或者它不是一个有效的目录，函数会记录一条错误信息并返回 None。
同样，如果类型列表为空，或者在尝试检索文件路径时发生任何其他错误，函数也会记录一条错误信息并返回 None。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393。保留所有权利。
"""

import logging

import os
from typing import List, Union, Optional

logger = logging.getLogger(__name__)


def get_file_paths_by_type(target_path: Union[str, os.PathLike], type_list: List[str]) -> Optional[List[str]]:
    """
    获取指定路径下特定类型文件的路径列表。

    :type target_path: Union[str, os.PathLike]
    :param target_path: 需要搜索的路径
    :type type_list: List[str]
    :param type_list: 拿来匹配的文件类型列表，例如 ['.txt', '.docx']
    :rtype: Optional[List[str]]
    :return: 匹配的文件路径列表，如果发生错误则返回 None。
    """
    try:
        if not os.path.exists(target_path):
            logger.error(f"The path '{target_path}' does not exist.")
            return None
        if not os.path.isdir(target_path):
            logger.error(f"'{target_path}' is not a valid directory.")
            return None
        if not type_list:
            logger.error(f"The file type list is empty.")
            return None

        type_list = [file_type.lower() for file_type in type_list]
        return [os.path.normpath(os.path.join(root, file)) for root, _, files in os.walk(target_path) for file in files if os.path.splitext(file)[1].lower() in type_list]
    except Exception:
        logger.exception("An error occurred while retrieving file paths")
        return None
