from typing import Union
from pathlib import Path


def rename_target_if_exist(path: Union[str, Path]) -> Path:
    """
    如果路径存在，则重命名。

    :param path: 需要重命名的路径
    :type path: Union[str, Path]
    :return: 重命名后的路径
    :rtype: Path
    :raise ValueError: 如果提供的路径为空或者不合法会抛出此异常
    """

    if not isinstance(path, Path):
        path = Path(path)

    # 检查路径是否为空或者不合法
    if path is None or str(path).strip() == '':
        raise ValueError("路径为空或者不合法.")

    original_path = path
    counter = 0
    while path.exists():
        counter += 1
        path = Path(f'{original_path}_({counter})')

    return path


if __name__ == "__main__":
    try:
        new_path = rename_target_if_exist('resources/new1')
        print(new_path)
    except Exception as e:
        print(str(e))
