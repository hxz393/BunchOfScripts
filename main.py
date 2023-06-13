import common


# noinspection NonAsciiCharacters
def 读取文本内容到列表():
    文本文件路径 = r'B:\2.脚本\新建文本文档.txt'
    返回列表 = common.read_txt_to_list(path=文本文件路径)
    print(返回列表)


if __name__ == '__main__':
    ##############常用############
    读取文本内容到列表()
