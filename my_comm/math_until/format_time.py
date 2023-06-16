from typing import Union

def format_time(seconds: Union[int, float]) -> str:
    """
    将秒数格式化为 "Xh Xm Xs" 格式的字符串。

    :type seconds: Union[int, float]
    :param seconds: 需要转换的秒数，可以是整数或浮点数。
    :rtype: str
    :return: 返回格式化后的字符串，格式为 "Xh Xm Xs"。
    :raise ValueError: 如果输入的秒数为负数，抛出 ValueError。
    """
    if seconds < 0:
        raise ValueError("Seconds cannot be negative.")

    try:
        hours, rem = divmod(int(seconds), 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{hours}h {minutes}m {seconds}s"
    except Exception as e:
        raise Exception(f"An error occurred while formatting time: {e}")

if __name__ == '__main__':
    try:
        print(format_time(65))
        print(format_time(3600))
        print(format_time(3661.67))
    except Exception as e:
        print(e)
