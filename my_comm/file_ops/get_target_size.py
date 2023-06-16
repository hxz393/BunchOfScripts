import os
from typing import Union
from concurrent import futures

def get_file_size(file_path: Union[str, os.PathLike]) -> int:
    try:
        return os.path.getsize(file_path)
    except Exception as e:
        raise Exception(f"获取文件大小时发生错误: {e}")

def get_target_size(target_path: Union[str, os.PathLike]) -> int:
    """
    获取目标文件或文件夹的大小

    :param target_path: 文件或文件夹的路径，可以是 str 或 os.PathLike 对象
    :return: 文件或文件夹的大小（字节数）
    :raise ValueError: 如果路径不存在或者不是一个有效的文件/文件夹，抛出 ValueError
    :raise Exception: 如果在处理过程中出现其他问题，抛出一般性的 Exception
    """
    if not os.path.exists(target_path):
        raise ValueError(f"路径 '{target_path}' 不存在")

    try:
        if os.path.isfile(target_path):
            return os.path.getsize(target_path)
        elif os.path.isdir(target_path):
            with futures.ThreadPoolExecutor() as executor:
                sizes = executor.map(get_file_size, (os.path.join(dirpath, f) for dirpath, dirnames, filenames in os.walk(target_path) for f in filenames))
            return sum(sizes)
        else:
            raise ValueError(f"'{target_path}' 不是一个有效的文件或文件夹")
    except Exception as e:
        raise Exception(f"获取文件或文件夹大小时发生错误: {e}")

if __name__ == '__main__':
    try:
        target_file = 'resources/new.json'
        file_size = get_target_size(target_file)
        print(file_size)

        target_directory = 'resources'
        directory_size = get_target_size(target_directory)
        print(directory_size)
    except Exception as e:
        print(e)
