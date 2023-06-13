from pathlib import Path
from typing import Union

def get_target_size(path: Union[str, Path]) -> int:
    """
    获取指定文件或文件夹的大小

    :param path: 文件或文件夹的路径
    :return: 文件或文件夹的大小（字节数）
    """
    path = Path(str(path))
    total_size = 0

    try:
        if path.is_file():
            total_size = path.stat().st_size
        elif path.is_dir():
            total_size = sum(file.stat().st_size for file in path.rglob('*') if file.is_file())

    except FileNotFoundError:
        print(f"文件或文件夹 '{path}' 不存在")

    except PermissionError:
        print(f"无法访问文件或文件夹 '{path}'，权限错误")

    except Exception as e:
        print(f"获取文件或文件夹大小时发生错误: {e}")

    return total_size
