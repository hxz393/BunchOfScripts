"""
这是一个Python文件，包含一个函数：`langconv_cht_to_chs`。

`langconv_cht_to_chs`函数的目标是将繁体字转换为简体字。如果转换成功，返回转换后的简体字字符串。如果转换失败，返回None。

这个函数接受一个参数：
- `word`：需要被转换的繁体字字符串。

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
import traceback
from typing import Optional

from third_party.langconv.langconv import Converter

logger = logging.getLogger(__name__)


def langconv_cht_to_chs(word: str) -> Optional[str]:
    """
    将繁体字转换为简体字。

    :param word: 需要被转换的繁体字字符串
    :type word: str
    :return: 转换为简体字后的字符串。如果转换失败，返回 None。
    :rtype: Optional[str]
    """
    try:
        if not isinstance(word, str):
            logger.error("The input should be a string.")
            return None
        if not word:
            logger.error("The input string cannot be empty.")
            return None

        converter = Converter('zh-hans')
        simplified_word = converter.convert(word)
        simplified_word.encode('utf-8')
        return simplified_word
    except Exception as e:
        logger.error(f"An error occurred during the conversion: {e}\n{traceback.format_exc()}")
        return None
