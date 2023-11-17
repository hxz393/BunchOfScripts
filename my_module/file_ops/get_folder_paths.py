"""
这个Python文件包含一个名为 `get_folder_paths` 的函数，其目的是获取目标目录下所有子目录的路径。

在给出目标路径的情况下，此函数将遍历目标路径下的所有子目录，并以字符串列表的形式返回所有子目录的路径。使用示例如下：

```python
folder_paths = get_folder_paths("/path/to/directory")
if folder_paths:
    for path in folder_paths:
        print(path)
```

如果目标路径不存在，或者它不是一个有效的目录，函数会记录一条错误信息并返回 None。同样，如果在尝试访问目录或检测子目录路径时发生任何错误，函数也会记录一条错误信息并返回 None。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393。保留所有权利。
"""
import os
from typing import List, Optional, Union
import logging


logger = logging.getLogger(__name__)


def get_folder_paths(target_path: Union[str, os.PathLike]) -> Optional[List[str]]:
    """
    获取目标目录下扫描到的所有文件夹路径

    :type target_path: Union[str, os.PathLike]
    :param target_path: 目标目录的路径，可以是字符串或 os.PathLike 对象。
    :rtype: Optional[List[str]]
    :return: 文件夹路径列表，如果出现错误，则返回 None。
    """

    try:
        target_path = os.path.normpath(target_path)
        if not os.path.exists(target_path):
            logger.error(f"The path '{target_path}' does not exist.")
            return None
        if not os.path.isdir(target_path):
            logger.error(f"'{target_path}' is not a valid directory.")
            return None

        return [os.path.normpath(os.path.join(root, dir_name)) for root, dirs, _ in os.walk(target_path) for dir_name in dirs]
    except Exception:
        logger.exception(f"An error occurred while retrieving folder paths")
        return None
