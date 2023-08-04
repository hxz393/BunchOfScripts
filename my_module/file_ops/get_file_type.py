"""
这个Python文件包含一个名为 `get_file_type` 的函数，用于通过读取文件头部内容来确定指定文件的真实类型。

给定一个目标文件的路径，此函数会尝试打开和读取该文件的前1024字节，并使用 `magic` 库来确定文件的类型。得到的文件类型将作为一个 MIME 类型的字符串返回。

函数的使用方式如下：

```python
file_type = get_file_type("/path/to/file")
if file_type:
    print(f"The file type is {file_type}")
```

如果目标路径不存在，或者它不是一个有效的文件，函数会记录一条错误信息并返回 None。
同样，如果在尝试访问文件（例如由于权限问题）或检测文件类型时发生任何其他错误，函数也会记录一条错误信息并返回 None。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393。保留所有权利。
"""

import os
import magic
from typing import Optional, Union
import logging
import traceback

logger = logging.getLogger(__name__)


def get_file_type(target_path: Union[str, os.PathLike]) -> Optional[str]:
    """
    以读取文件头部内容的方式，取得指定文件的真实类型

    :type target_path: Union[str, os.PathLike]
    :param target_path: 要检测的文件路径，可以是字符串或 os.PathLike 对象。
    :rtype: Optional[str]
    :return: 文件类型检测结果，如果检测失败则返回 None。
    """
    try:
        target_path = os.path.normpath(target_path)
        if not os.path.exists(target_path):
            logger.error(f"The file '{target_path}' does not exist.")
            return None
        if not os.path.isfile(target_path):
            logger.error(f"'{target_path}' is not a valid file.")
            return None

        with open(target_path, 'rb') as f:
            return magic.from_buffer(f.read(1024), mime=True)
    except PermissionError:
        logger.error(f"Unable to access file '{target_path}', permission denied.")
        return None
    except Exception as e:
        logger.error(f"An error occurred while detecting the file type: {e}\n{traceback.format_exc()}")
        return None
