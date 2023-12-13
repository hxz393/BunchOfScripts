"""
这是一个Python文件，其中包含一个函数：`calculate_transfer_speed`。

`calculate_transfer_speed`函数的主要目标是根据传输的字节数和消耗的时间，计算文件传输速度并以易读的格式返回。这是一种常见的功能，可以在进行文件传输或网络传输等操作时计算和显示传输速度。该函数接受两个参数：`size_bytes` 和 `elapsed_time_seconds`。 `size_bytes` 是传输的字节数，而 `elapsed_time_seconds` 是传输所需的时间，单位为秒。

函数首先对输入参数进行一些基本的类型和值检查，确保它们是有效的。然后，它计算速度（字节/秒），并通过一个循环逐步将速度转换为更高的单位，直到找到一个合适的单位使得速度小于1024。最后，它返回一个字符串，包含了速度值和单位。

如果在任何时候发生异常，函数将使用 `logging` 模块记录错误，并返回 `None`。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import logging

from typing import Union, Optional

logger = logging.getLogger(__name__)


def calculate_transfer_speed(size_bytes: int, elapsed_time_seconds: Union[int, float]) -> Optional[str]:
    """
    根据传输的字节数和消耗的时间，计算文件传输速度并以易读的格式返回。

    :param size_bytes: 传输的字节数。
    :type size_bytes: int
    :param elapsed_time_seconds: 消耗的时间，单位是秒。
    :type elapsed_time_seconds: Union[int, float]
    :return: 文件传输速度的字符串格式，例如 "23.4 MB/s"，若发生错误则返回 None。
    :rtype: Optional[str]
    """
    try:
        if not isinstance(size_bytes, int):
            raise TypeError(f"Size_bytes should be int, but got {type(size_bytes)}")
        if not isinstance(elapsed_time_seconds, (int, float)):
            raise TypeError(f"Elapsed_time_seconds should be int or float, but got {type(elapsed_time_seconds)}")
        if size_bytes < 0:
            raise ValueError(f"Size_bytes should not be negative, but got {size_bytes}")
        if elapsed_time_seconds <= 0:
            raise ValueError(f"Elapsed_time_seconds should be positive, but got {elapsed_time_seconds}")

        speed = size_bytes / elapsed_time_seconds

        units = ["Bytes", "KB", "MB", "GB", "TB"]
        for unit in units:
            if speed < 1024:
                return f"{speed:.2f} {unit}/s"
            speed /= 1024

        return f"{speed:.2f} {units[-1]}/s"
    except Exception:
        logger.exception("An error occurred while calculating transfer speed")
        return None
