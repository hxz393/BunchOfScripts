import os
from typing import List


def get_file_paths_by_type(target_path: str, type_list: List[str]) -> List[str]:
    """
    获取指定路径下特定类型文件的路径列表。

    :param target_path: 需要搜索的路径
    :param type_list: 文件类型列表
    :return: 文件路径列表
    :raise FileNotFoundError: 如果指定路径不存在会抛出此异常
    :raise ValueError: 如果文件类型列表为空会抛出此异常
    """
    if not os.path.exists(target_path):
        raise FileNotFoundError(f"指定路径 {target_path} 不存在.")

    # 检查文件类型列表是否为空
    if not type_list:
        raise ValueError(f"文件类型列表为空.")

    type_list = [i.lower() for i in type_list]

    file_paths = []

    for root, _, files in os.walk(target_path):
        for file in files:
            if os.path.splitext(file)[1].lower() in type_list:
                file_paths.append(os.path.join(root, file))

    return file_paths


if __name__ == "__main__":
    目标目录 = r'resources/'
    要筛选文件类型列表 = ['.txt', '.pdf']
    try:
        筛选出的文件列表 = get_file_paths_by_type(target_path=目标目录, type_list=要筛选文件类型列表)
        print(筛选出的文件列表)
    except Exception as e:
        print(str(e))
