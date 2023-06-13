import common


# noinspection NonAsciiCharacters
def 读取文件内容到列表():
    文本文件路径 = r'B:\2.脚本\新建文本文档.txt'
    返回列表 = common.read_file_to_list(path = 文本文件路径)
    print(返回列表)


def 写入列表到文件():
    写入列表 = [1, 'a', '啊']
    文本文件路径 = r'B:\2.脚本\新建文本文档.txt'
    common.write_list_to_file(path = 文本文件路径, content = 写入列表)


def 获取目标目录扫描到的所有文件列表():
    目标目录 = r'B:\2.脚本'
    返回列表 = common.get_file_paths(path = 目标目录)
    print(返回列表)


def 获取目标目录扫描到的所有文件夹列表():
    目标目录 = r'B:\2.脚本'
    返回列表 = common.get_folder_paths(path = 目标目录)
    print(返回列表)

def 获取目标目录下第一级文件夹列表():
    目标目录 = r'B:\2.脚本'
    返回列表 = common.get_subdirectories(path = 目标目录)
    print(返回列表)


def 获取文件类型依据文件内容():
    目标文件 = r'B:\2.脚本\新建文本文档.txt'
    返回类型 = common.get_file_type(path = 目标文件)
    print(返回类型)


def 获取文件或文件夹大小():
    目标文件 = r'B:\2.脚本\新建文本文档.txt'
    文件大小 = common.get_target_size(path = 目标文件)
    print(文件大小)
    目标目录 = r'B:\2.脚本'
    目录大小 = common.get_target_size(path = 目标目录)
    print(目录大小)