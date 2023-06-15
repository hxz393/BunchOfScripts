def calculate_transfer_speed(size_bytes: int, elapsed_time_seconds: float) -> str:
    """
    根据传输的字节数和消耗的时间，计算文件传输速度并以易读的格式返回。

    :param size_bytes: 传输的字节数
    :type size_bytes: int
    :param elapsed_time_seconds: 消耗的时间，单位是秒
    :type elapsed_time_seconds: float
    :raise TypeError: 如果输入的字节数不是整数或输入的时间不是浮点数或整数。
    :raise ValueError: 如果输入的字节数或时间是负数。
    :return: 文件传输速度的字符串格式，例如 "23.4 MB/s"
    :rtype: str
    """
    if not isinstance(size_bytes, int):
        raise TypeError(f"输入的字节数 {size_bytes} 类型应为整数")
    if not isinstance(elapsed_time_seconds, (float, int)):
        raise TypeError(f"输入的时间 {elapsed_time_seconds} 类型应为浮点数或整数")
    if size_bytes < 0:
        raise ValueError(f"输入的字节数 {size_bytes} 不应为负数")
    if elapsed_time_seconds <= 0:
        raise ValueError(f"输入的时间 {elapsed_time_seconds} 应为正数")

    # 计算每秒传输的字节数
    bytes_per_second = size_bytes / elapsed_time_seconds

    # 定义数据量单位列表
    units = ["Bytes", "KB", "MB", "GB"]
    # 计算所属数据量级
    index = 0
    while bytes_per_second >= 1024 and index < len(units) - 1:
        bytes_per_second /= 1024
        index += 1

    # 格式化传输速度
    speed_formatted = f"{bytes_per_second:.2f} {units[index]}/s"

    return speed_formatted


if __name__ == '__main__':
    try:
        print(calculate_transfer_speed(1048576, 2))  # 输出：'512.00 KB/s'
        print(calculate_transfer_speed(5368709120, 180))  # 输出：'28.44 MB/s'
        print(calculate_transfer_speed(100000000000, 10))  # 输出：'9.31 GB/s'
    except Exception as e:
        print(e)
