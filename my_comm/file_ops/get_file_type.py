import magic
from typing import Optional

def get_file_type(path: str) -> Optional[str]:
    """
    以读取文件头部内容的方式，取得指定文件的真实类型

    :param path: 要检测的文件路径，仅接受 str 类型
    :return: 文件类型检测结果，如果检测失败则返回 None
    :raise ValueError: 如果路径不是一个有效的文件，抛出 ValueError
    :raise Exception: 如果在处理过程中出现其它问题，抛出一般性的 Exception
    """
    if not isinstance(path, str):
        raise ValueError(f"路径参数必须为 str 类型，而非 {type(path).__name__}")

    try:
        with open(path, 'rb') as f:
            file_type = magic.from_buffer(f.read(1024), mime=True)
    except FileNotFoundError:
        raise ValueError(f"文件 '{path}' 不存在")
    except PermissionError:
        raise PermissionError(f"无法访问文件 '{path}'，权限错误")
    except Exception as e:
        raise Exception(f"检测文件类型时发生错误: {e}")

    return file_type


if __name__ == '__main__':
    目标文件 = r'resources\new.json'
    try:
        返回类型 = get_file_type(path=目标文件)
        print(返回类型)
    except Exception as e:
        print(e)
