def format_time(duration: float, decimal_places: int = 2) -> str:
    """
    将时间（秒）格式化为易于理解的格式（秒，分钟，小时）。

    :param duration: 时间长度，单位为秒。
    :type duration: float
    :param decimal_places: 转换后的数值的精度（小数点后的位数）。
    :type decimal_places: int, default 2
    :raise TypeError: 如果输入的时间长度不是浮点数或整数。
    :raise ValueError: 如果输入的时间长度是负数。
    :return: 格式化后的时间长度（字符串格式）。
    """
    FORMAT_LIST = ["s", "m", "h"]
    UNIT = 60.0

    # 检查 duration 的类型和值
    if not isinstance(duration, (float, int)):
        raise TypeError(f"输入的时间长度 {duration} 类型应为浮点数或整数")
    if duration < 0:
        raise ValueError(f"输入的时间长度 {duration} 不应为负数")

    time_str = ""
    for fmt in FORMAT_LIST:
        duration, remainder = divmod(duration, UNIT)
        if fmt == "s":
            remainder = round(remainder, decimal_places)
            time_str = f'{remainder}{fmt}' + ' ' + time_str
        elif remainder > 0:
            time_str = f'{int(remainder)}{fmt} ' + time_str

    return time_str.strip()


if __name__ == '__main__':
    try:
        print(format_time(65))  # 输出：'1m 5.0s'
        print(format_time(3600))  # 输出：'1h 0.0s'
        print(format_time(3661.67, decimal_places=3))  # 输出：'1h 1m 1.67s'
    except Exception as e:
        print(e)
