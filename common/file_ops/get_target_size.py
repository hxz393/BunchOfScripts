from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Union


def get_file_size(path: Path) -> int:
    return path.stat().st_size


def get_target_size(path: Union[str, Path]) -> int:
    """
    获取目标文件或文件夹的大小

    :param path: 文件或文件夹的路径
    :return: 文件或文件夹的大小（字节数）
    """
    path = Path(str(path))
    total_size = 0

    try:
        if path.is_file():
            total_size = path.stat().st_size
        elif path.is_dir():
            with ThreadPoolExecutor() as executor:
                files = list(path.rglob('*'))
                sizes = executor.map(get_file_size, files)
                total_size = sum(sizes)

    except FileNotFoundError:
        print(f"文件或文件夹 '{path}' 不存在")

    except PermissionError:
        print(f"无法访问文件或文件夹 '{path}'，权限错误")

    except Exception as e:
        print(f"获取文件或文件夹大小时发生错误: {e}")

    return total_size


if __name__ == '__main__':
    目标文件 = r'resources\new.json'
    文件大小 = get_target_size(path=目标文件)
    print(文件大小)
    目标目录 = r'resources'
    目录大小 = get_target_size(path=目标目录)
    print(目录大小)
