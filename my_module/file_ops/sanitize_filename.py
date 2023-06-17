import re


# noinspection PyShadowingNames
def sanitize_filename(filename: str) -> str:
    """
    检查一个字符串是否可以作为Windows上的文件名，如果不行则替换掉不能作为文件名的字符为-。

    :param filename: 待检查的字符串
    :type filename: str
    :return: 能作为文件名的字符串
    :rtype: str
    """
    forbidden_chars = r'[\/:*?"<>|]'
    if re.search(forbidden_chars, filename):
        filename = re.sub(forbidden_chars, '-', filename)
    return filename
