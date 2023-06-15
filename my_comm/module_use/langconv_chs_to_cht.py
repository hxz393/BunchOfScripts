from third_party.langconv.langconv import Converter
from typing import Optional


def langconv_chs_to_cht(word: str) -> Optional[str]:
    """
    将简体字转换为繁体字。

    :param word: 需要被转换的简体字字符串
    :type word: str
    :return: 转换为繁体字后的字符串
    :rtype: Optional[str]
    :raise TypeError: 如果输入的不是字符串类型，将抛出此异常
    :raise ValueError: 如果输入的字符串为空，将抛出此异常
    """
    if not isinstance(word, str):
        raise TypeError("输入的参数应为字符串类型")

    if not word:
        raise ValueError("输入的字符串不能为空")

    try:
        converter = Converter('zh-hant')
        traditional_word = converter.convert(word)
        traditional_word.encode('utf-8')
        return traditional_word
    except Exception as e:
        print(f"在转换过程中发生错误：{str(e)}")
        return None


if __name__ == "__main__":
    简体字符串 = '转换简体到繁体'
    繁体字符串 = langconv_chs_to_cht(简体字符串)
    print(繁体字符串)
