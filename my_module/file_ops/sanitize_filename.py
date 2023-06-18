import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

def sanitize_filename(filename: str) -> Optional[str]:
    """
    检查一个字符串是否可以作为Windows上的文件名，如果不行则替换掉不能作为文件名的字符为-。

    :param filename: 待检查的字符串
    :type filename: str
    :return: 能作为文件名的字符串，如果出错则返回 None。
    :rtype: Optional[str]
    """
    try:
        forbidden_chars = r'[\/:*?"<>|]'
        if re.search(forbidden_chars, filename):
            filename = re.sub(forbidden_chars, '-', filename)
        return filename
    except Exception as e:
        logger.error(f"An error occurred while sanitizing the filename: {e}")
        return None
