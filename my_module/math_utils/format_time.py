"""
这是一个Python文件，其中包含一个函数：`format_time`。

`format_time`函数的目标是将给定的秒数格式化为 "Xh Xm Xs" 格式的字符串。它接受一个参数，`seconds`，这应该是需要转换的秒数，可以是整数或浮点数。

在函数体内，首先检查`seconds`是否小于0。如果不是，则将`seconds`分解成小时、分钟和秒，并返回一个格式化的字符串。如果在任何时候发生异常，函数将使用`logging`模块记录错误，并返回`None`。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import logging
import traceback
from typing import Union, Optional

logger = logging.getLogger(__name__)


def format_time(seconds: Union[int, float]) -> Optional[str]:
    """
    将秒数格式化为 "Xh Xm Xs" 格式的字符串。

    :param seconds: 需要转换的秒数，可以是整数或浮点数。
    :type seconds: Union[int, float]
    :return: 返回格式化后的字符串，格式为 "Xh Xm Xs"，若发生错误则返回 None。
    :rtype: Optional[str]
    """
    try:
        if seconds < 0:
            raise ValueError("Seconds cannot be negative.")

        hours, rem = divmod(int(seconds), 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{hours}h {minutes}m {seconds}s"
    except Exception as e:
        logger.error(f"An error occurred while formatting time: {e}\n{traceback.format_exc()}")
        return None
