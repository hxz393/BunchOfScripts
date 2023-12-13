"""
这是一个Python文件，包含两个函数：`remove_temp_file`和`convert_base64_to_ico`。

`remove_temp_file`函数的目标是删除指定路径的临时文件。它接受一个参数：
- `path`：需要被删除的临时文件的路径。

`convert_base64_to_ico`函数的目标是将Base64字符串解码并保存为.ico文件。它接受一个参数：
- `base64_string`：Base64编码的字符串。
函数返回生成的.ico文件的路径，如果过程中有错误发生，返回 None。

此文件依赖于以下Python库：
- `base64`
- `os`
- `tempfile`
- `atexit`
- `logging`

函数使用了日志记录器记录任何在转换过程中发生的错误。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import atexit
import base64
import logging

import os
import tempfile
from typing import Optional

logger = logging.getLogger(__name__)


def remove_temp_file(path: str):
    """删除临时文件"""
    try:
        os.remove(path)
    except Exception:
        logger.exception("Error when remove temp file")
        pass


def convert_base64_to_ico(base64_string: str) -> Optional[str]:
    """
    将Base64字符串解码并保存为.ico文件。

    :param base64_string: Base64编码的字符串。
    :type base64_string: str
    :return: 生成的.ico文件的路径，如果过程中有错误发生，返回 None。
    :rtype: Optional[str]
    """
    try:
        icon_data = base64.b64decode(base64_string)
    except Exception:
        logger.exception("The input string cannot be decoded by Base64")
        return None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.ico') as temp_file:
            temp_file.write(icon_data)
        atexit.register(remove_temp_file, temp_file.name)
        return temp_file.name
    except Exception:
        logger.exception("An error occurred while writing the .ico file")
        return None
