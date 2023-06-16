import os
from typing import Union

def calculate_transfer_speed(size_bytes: int, elapsed_time_seconds: Union[int, float]) -> str:
    """
    根据传输的字节数和消耗的时间，计算文件传输速度并以易读的格式返回。

    :type size_bytes: int
    :param size_bytes: 传输的字节数。
    :type elapsed_time_seconds: Union[int, float]
    :param elapsed_time_seconds: 消耗的时间，单位是秒。
    :rtype: str
    :return: 文件传输速度的字符串格式，例如 "23.4 MB/s"。
    :raise TypeError: 如果输入的字节数不是整数或输入的时间不是浮点数或整数，抛出 TypeError。
    :raise ValueError: 如果输入的字节数或时间是负数或零，抛出 ValueError。
    """
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

if __name__ == '__main__':
    try:
        print(calculate_transfer_speed(1024, 1))
        print(calculate_transfer_speed(1024, 0.5))
        print(calculate_transfer_speed(198548576, 1))
        print(calculate_transfer_speed(11173741824, 19))
    except Exception as e:
        print(e)
