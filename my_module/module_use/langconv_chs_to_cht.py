"""
这是一个Python文件，包含一个函数：`langconv_chs_to_cht`。

`langconv_chs_to_cht`函数的目标是将简体字转换为繁体字。如果转换成功，返回转换后的繁体字字符串。如果转换失败，返回None。

这个函数接受一个参数：
- `word`：需要被转换的简体字字符串。

此文件依赖于以下Python库：
- `logging`
- `third_party.langconv.langconv`
- `typing`

函数使用了日志记录器记录任何在转换过程中发生的错误。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import logging

from typing import Optional

from third_party.langconv.langconv import Converter

logger = logging.getLogger(__name__)


def langconv_chs_to_cht(word: str) -> Optional[str]:
    """
    将简体字转换为繁体字。

    :param word: 需要被转换的简体字字符串
    :type word: str
    :return: 转换为繁体字后的字符串。如果转换失败，返回 None。
    :rtype: Optional[str]
    """
    try:
        if not isinstance(word, str):
            logger.error("The input should be a string.")
            return None
        if not word:
            logger.error("The input string cannot be empty.")
            return None

        converter = Converter('zh-hant')
        traditional_word = converter.convert(word)
        traditional_word.encode('utf-8')
        return traditional_word
    except Exception:
        logger.exception(f"An error occurred during the conversion")
        return None
