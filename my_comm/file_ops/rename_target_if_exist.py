from typing import Union
from pathlib import Path


def rename_target_if_exist(path: Union[str, Path]) -> Path:
    """
    如果目标路径存在，则重命名。

    :param path: 需要重命名的路径
    :type path: Union[str, Path]
    :return: 重命名后的路径
    :rtype: Path
    :raise ValueError: 如果提供的路径为空或者不合法会抛出此异常
    :raise FileNotFoundError: 如果路径不存在，会抛出此异常
    """

    if not isinstance(path, Path):
        path = Path(path)

    # 检查路径是否为空或者不合法
    if path is None or str(path).strip() == '':
        raise ValueError("The path is empty or invalid.")
    # 检查路径是否存在
    if not path.exists():
        raise FileNotFoundError(f"The path '{path}' does not exist.")

    original_path = path
    counter = 1
    while path.exists():
        path = original_path.with_stem(f"{original_path.stem}_({counter})")
        counter += 1

    return path


if __name__ == "__main__":
    try:
        new_path = rename_target_if_exist('resources/new1')
        print(new_path)
    except Exception as e:
        print(str(e))
