"""
这是一个Python文件，其中包含一个函数：`format_size`。

`format_size`函数的目的是将给定的大小（以字节为单位）转换为易于理解的格式，例如KB，MB，GB等。该函数接受三个参数：`size`，`is_disk`和`precision`。 `size`是以字节为单位的大小；`is_disk`是一个布尔值，表示该大小是否表示磁盘大小（如果是磁盘大小，则使用1000作为单位换算，否则使用1024）；`precision`定义了结果中的小数点后的位数。

在函数体中，首先对`size`的类型和值进行了检查。然后，函数通过循环，逐渐将`size`转换到更大的单位，直到`size`小于单位步长（`unit_step`，取决于`is_disk`）。最后，函数返回一个格式化的字符串，表示转换后的大小和单位。

如果在任何时候发生异常，函数将使用`logging`模块记录错误，并返回`None`。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import logging
import traceback
from typing import Union, Optional

logger = logging.getLogger(__name__)


def format_size(size: Union[int, float], is_disk: bool = False, precision: int = 2) -> Optional[str]:
    """
    将字节单位的文件或磁盘大小转换为易于理解的格式（KB, MB, GB等）。

    :param size: 文件或磁盘的大小，单位为字节。
    :type size: Union[int, float]
    :param is_disk: 是否是磁盘大小（如果是磁盘大小，则使用1000作为单位换算，否则使用1024）。
    :type is_disk: bool
    :param precision: 转换后的数值的精度（小数点后的位数）。
    :type precision: int
    :return: 格式化后的文件或磁盘大小（字符串格式），若发生错误则返回 None。
    :rtype: Optional[str]
    """
    try:
        if not isinstance(size, (float, int)):
            raise TypeError(f"Size should be float or int, but got {type(size)}")
        if size < 0:
            raise ValueError(f"Size should not be negative, but got {size}")

        units = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
        unit_step = 1000 if is_disk else 1024
        for unit in units:
            if abs(size) < unit_step:
                return f"{size:.{precision}f} {unit}"
            size /= unit_step

        return f"{size:.{precision}f} {units[-1]}"
    except Exception as e:
        logger.error(f"An error occurred while formatting size: {e}\n{traceback.format_exc()}")
        return None
