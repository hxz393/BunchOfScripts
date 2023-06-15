import re

def sanitize_filename(filename: str) -> str:
    """
    检查一个字符串是否可以作为Windows上的文件名，如果不行则替换掉不能作为文件名的字符。

    :param filename: 待检查的字符串
    :type filename: str
    :return: 能作为文件名的字符串
    :rtype: str
    """
    # Windows上不能用于文件名的字符
    forbidden_chars = r'[\/:*?"<>|]'
    if re.search(forbidden_chars, filename):
        # 如果找到这样的字符，则替换为-
        filename = re.sub(forbidden_chars, '-', filename)
    return filename

if __name__ == '__main__':
    # 使用示例
    print(sanitize_filename('filename?with/special*chars:'))  # 输出: filename-with-special-chars-
