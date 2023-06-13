import magic
from typing import Optional

def get_file_type(path: str) -> Optional[str]:
    """
    以读取文件头部内容的方式，取得指定文件的真实类型

    :param path: 要检测的文件路径
    :return: 文件类型检测结果，如果检测失败则返回 None
    """
    file_type = None

    try:
        with open(path, 'rb') as f:
            file_type = magic.from_buffer(f.read(1024), mime=True)

    except FileNotFoundError:
        print(f"文件 '{path}' 不存在")

    except PermissionError:
        print(f"无法访问文件 '{path}'，权限错误")

    except Exception as e:
        print(f"检测文件类型时发生错误: {e}")

    return file_type
