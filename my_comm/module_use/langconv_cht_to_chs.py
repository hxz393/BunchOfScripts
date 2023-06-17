from third_party.langconv.langconv import Converter
from typing import Optional

def langconv_cht_to_chs(word: str) -> Optional[str]:
    """
    将繁体字转换为简体字。

    :param word: 需要被转换的繁体字字符串
    :type word: str
    :rtype: Optional[str]
    :raise TypeError: 如果输入的不是字符串类型，将抛出此异常。
    :raise ValueError: 如果输入的字符串为空，将抛出此异常。
    :raise Exception: 如果在转换过程中发生错误，将抛出此异常。
    :return: 转换为简体字后的字符串。如果转换失败，返回 None。
    """
    if not isinstance(word, str):
        raise TypeError("The input should be a string.")
    if not word:
        raise ValueError("The input string cannot be empty.")

    try:
        converter = Converter('zh-hans')
        simplified_word = converter.convert(word)
        simplified_word.encode('utf-8')
        return simplified_word
    except Exception as e:
        raise Exception(f"An error occurred during the conversion: {e}")

if __name__ == "__main__":
    traditional_string = '轉換繁體到簡體'
    try:
        simplified_string = langconv_cht_to_chs(traditional_string)
        print(simplified_string)
    except Exception as e:
        print(f"An error occurred: {e}")
