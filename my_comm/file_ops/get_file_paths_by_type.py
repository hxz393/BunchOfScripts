from pathlib import Path
from typing import List, Union


def get_file_paths_by_type(path: Union[str, Path], type_list: List[str]) -> List[str]:
    """
    获取指定路径下特定类型文件的路径列表。

    :param path: 需要搜索的路径
    :type path: Union[str, Path]
    :param type_list: 文件类型列表
    :type type_list: List[str]
    :return: 文件路径列表
    :rtype: List[str]
    :raise FileNotFoundError: 如果指定路径不存在会抛出此异常
    :raise ValueError: 如果文件类型列表为空会抛出此异常
    """

    if not isinstance(path, Path):
        path = Path(path)

    # 检查路径是否存在
    if not path.exists():
        raise FileNotFoundError(f"指定路径 {path} 不存在.")

    # 检查文件类型列表是否为空
    if not type_list:
        raise ValueError(f"文件类型列表为空.")

    type_list = [i.lower() for i in type_list]

    # 使用生成器表达式提升性能
    file_paths = [str(file_path) for file_path in path.glob('**/*') if file_path.is_file() and file_path.suffix.lower() in type_list]

    return file_paths


if __name__ == "__main__":
    try:
        file_paths = get_file_paths_by_type(r'resources/', ['.txt', '.pdf'])
        print(file_paths)
    except Exception as e:
        print(str(e))
