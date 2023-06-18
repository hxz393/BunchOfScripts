from typing import Union, Optional
import logging

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
        logger.error(f"An error occurred while formatting time: {e}")
        return None
