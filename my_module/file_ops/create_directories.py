"""
这个Python文件提供了一个功能，即根据给定的路径列表创建不存在的目录。

它包含了一个名为 `create_directories` 的函数。这个函数接受一个目录路径列表作为参数，
遍历列表中的每个路径，如果路径代表的目录不存在，就创建它。

函数的使用方式如下：

```python
directories = create_directories(["path/to/dir1", "path/to/dir2"])
print(directories)  # 输出成功创建的目录列表
```

请注意，函数会检查每个路径的有效性和长度。如果路径无效或过长，函数将记录一条错误信息并跳过该路径。
函数还会处理在创建目录时可能发生的任何错误，如果无法创建目录，函数将记录一条错误信息并跳过该路径。

在返回时，函数提供一个列表，包含所有成功创建的目录的路径。如果输入的路径列表为空，函数将记录一条错误信息并返回 None。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393。保留所有权利。
"""
import logging
import os
from typing import List, Optional

logger = logging.getLogger(__name__)


def create_directories(path_list: List[str]) -> Optional[List[str]]:
    """
    创建不存在的目录。

    :param path_list: 目录路径列表。
    :type path_list: List[str]
    :return: 成功创建的目录列表，如果输入列表为空，则返回None。
    :rtype: Optional[List[str]]
    """
    if not path_list:
        logger.error("The directory list is empty.")
        return None

    success_list = []
    for path in path_list:
        path = os.path.normpath(path)

        filename = os.path.split(path)[-1]
        if len(path) > 260 or any(char in filename for char in r'<>:"/\|?*'):
            logger.error(f"The directory path {path} is invalid.")
            continue

        if not os.path.exists(path):
            try:
                os.makedirs(path)
                success_list.append(path)
            except OSError:
                logger.error(f"Failed to create the directory {path}.")
                continue

    return success_list
