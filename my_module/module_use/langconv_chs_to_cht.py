from third_party.langconv.langconv import Converter
from typing import Optional


# noinspection PyShadowingNames
def langconv_chs_to_cht(word: str) -> Optional[str]:
    """
    将简体字转换为繁体字。

    :param word: 需要被转换的简体字字符串
    :type word: str
    :rtype: Optional[str]
    :raise TypeError: 如果输入的不是字符串类型，将抛出此异常。
    :raise ValueError: 如果输入的字符串为空，将抛出此异常。
    :raise Exception: 如果在转换过程中发生错误，将抛出此异常。
    :return: 转换为繁体字后的字符串。如果转换失败，返回 None。
    """
    if not isinstance(word, str):
        raise TypeError("The input should be a string.")
    if not word:
        raise ValueError("The input string cannot be empty.")

    try:
        converter = Converter('zh-hant')
        traditional_word = converter.convert(word)
        traditional_word.encode('utf-8')
        return traditional_word
    except Exception as e:
        raise Exception(f"An error occurred during the conversion: {e}")
