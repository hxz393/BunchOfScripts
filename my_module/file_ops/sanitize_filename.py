"""
这是一个Python文件，其中包含一个函数：`sanitize_filename`。

函数 `sanitize_filename` 用于检查字符串是否可以作为Windows系统上的文件名，如果含有不能作为文件名的字符，则将这些字符替换为'-'。它接受一个参数 `filename`，这是需要检查的字符串。函数首先定义了一个字符串 `forbidden_chars`，它包含了在Windows系统上不能作为文件名的字符。然后，函数使用正则表达式 `re.search` 检查 `filename` 是否包含 `forbidden_chars` 中的任何字符。如果找到这样的字符，函数就使用 `re.sub` 替换这些字符为'-'。如果所有操作都成功，函数将返回经过清洗的 `filename`，否则返回 `None`。

这个模块主要用于处理文件名，包括检查文件名是否合法，并在需要时清洗文件名。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import logging

import re
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
        forbidden_chars = r'[\\\/:*?"<>|]'
        if re.search(forbidden_chars, filename):
            filename = re.sub(forbidden_chars, '-', filename)
        return filename
    except Exception:
        logger.exception("An error occurred while sanitizing the filename")
        return None
