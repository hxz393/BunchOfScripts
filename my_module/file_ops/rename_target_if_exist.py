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
    """

    if not isinstance(path, Path):
        path = Path(path)

    if path is None or str(path).strip() == '':
        raise ValueError("The path is empty or invalid.")

    original_path = path
    counter = 1
    while path.exists():
        path = original_path.with_stem(f"{original_path.stem}_({counter})")
        counter += 1

    return path