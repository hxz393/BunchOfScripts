from typing import Union


def format_size(size: Union[int, float], is_disk: bool = False, precision: int = 2) -> str:
    """
    将字节单位的文件或磁盘大小转换为易于理解的格式（KB, MB, GB等）。

    :type size: Union[int, float]
    :param size: 文件或磁盘的大小，单位为字节。
    :type is_disk: bool
    :param is_disk: 是否是磁盘大小（如果是磁盘大小，则使用1000作为单位换算，否则使用1024）。
    :type precision: int
    :param precision: 转换后的数值的精度（小数点后的位数）。
    :rtype: str
    :return: 格式化后的文件或磁盘大小（字符串格式）。
    :raise TypeError: 如果输入的大小不是浮点数或整数，抛出 TypeError。
    :raise ValueError: 如果输入的大小是负数，抛出 ValueError。
    """
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
