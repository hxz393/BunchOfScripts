"""
这个Python文件包含一个名为 `get_subdirectories` 的函数，它用于获取给定目录下的第一级（直接）子目录列表。你可以通过调用此函数并提供目标路径（可以是`str`或`os.PathLike`对象）来使用它。

例如，如果你有一个包含多个子目录的目录，并且你想获取所有这些子目录的路径列表，你可以这样使用此函数：

```python
subdirs = get_subdirectories("/path/to/your/directory")
if subdirs:
    for subdir in subdirs:
        print(subdir)
```

如果提供的路径不存在，或者它不是一个有效的目录，函数会记录一个错误消息并返回 None。同样，如果在获取子目录时发生任何其他错误，函数也会记录错误并返回 None。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393。保留所有权利。
"""
import logging
import traceback
import os
from typing import List, Union, Optional

logger = logging.getLogger(__name__)


def get_subdirectories(target_path: Union[os.PathLike, str]) -> Optional[List[str]]:
    """
    获取目标目录下的第一级目录路径列表。

    :param target_path: 检测目录，可以是 str 或 os.PathLike 对象。
    :type target_path: Union[os.PathLike, str]
    :return: 文件夹路径列表或者None。
    :rtype: Optional[List[str]]
    """
    try:
        if not os.path.exists(target_path):
            logger.error(f"The path '{target_path}' does not exist.")
            return None
        if not os.path.isdir(target_path):
            logger.error(f"'{target_path}' is not a valid directory.")
            return None
        return [entry.path for entry in os.scandir(target_path) if entry.is_dir()]
    except Exception as e:
        logger.error(f"An error occurred while getting subdirectories: {e}\n{traceback.format_exc()}")
        return None
