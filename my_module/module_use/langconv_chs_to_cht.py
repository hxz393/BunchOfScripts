import logging
from third_party.langconv.langconv import Converter
from typing import Optional

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
    except Exception as e:
        logger.error(f"An error occurred during the conversion: {e}")
        return None
