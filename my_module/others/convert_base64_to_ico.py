import base64
import tempfile
from typing import Optional


# noinspection PyShadowingNames
def convert_base64_to_ico(base64_string: str) -> Optional[str]:
    """
    将Base64字符串解码并保存为.ico文件。

    :param base64_string: Base64编码的字符串。
    :type base64_string: str
    :rtype: Optional[str]
    :return: 生成的.ico文件的路径，如果过程中有错误发生，返回 None。
    :raise ValueError: 如果输入的字符串无法被Base64解码会抛出此异常。
    :raise IOError: 写入.ico文件时发生错误。
    """

    try:
        icon_data = base64.b64decode(base64_string)
    except Exception as e:
        raise ValueError(f"The input string cannot be decoded by Base64: {str(e)}")

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.ico') as temp_file:
            temp_file.write(icon_data)
        return temp_file.name
    except Exception as e:
        raise IOError(f"An error occurred while writing the .ico file: {str(e)}")