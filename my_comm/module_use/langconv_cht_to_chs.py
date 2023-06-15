from third_party.langconv.langconv import Converter
from typing import Optional


def langconv_cht_to_chs(word: str) -> Optional[str]:
    """
    将繁体字转换为简体字。

    :param word: 需要被转换的繁体字字符串
    :type word: str
    :return: 转换为简体字后的字符串
    :rtype: Optional[str]
    :raise TypeError: 如果输入的不是字符串类型，将抛出此异常
    :raise ValueError: 如果输入的字符串为空，将抛出此异常
    """
    if not isinstance(word, str):
        raise TypeError("输入的参数应为字符串类型")

    if not word:
        raise ValueError("输入的字符串不能为空")

    try:
        converter = Converter('zh-hans')
        simplified_word = converter.convert(word)
        simplified_word.encode('utf-8')
        return simplified_word
    except Exception as e:
        print(f"在转换过程中发生错误：{str(e)}")
        return None


if __name__ == "__main__":
    繁体字符串 = '轉換繁體到簡體'
    简体字符串 = langconv_cht_to_chs(繁体字符串)
    print(简体字符串)
