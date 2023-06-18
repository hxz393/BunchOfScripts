import logging
from third_party.langconv.langconv import Converter
from typing import Optional

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
        logger.error(f"An error occurred during the conversion: {e}")
        return None
