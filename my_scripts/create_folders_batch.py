"""
此 Python 文件包含一个名为 `create_folders_batch` 的函数，用于根据文本文件中的名称列表在指定目录中批量创建文件夹。

`create_folders_batch` 函数接收两个参数，`target_path` 和 `txt_file`。`target_path` 是期望创建文件夹的目标目录，
而 `txt_file` 是包含希望创建的文件夹名称的文本文件的路径。

函数首先会检查 `target_path` 是否存在，如果不存在，则记录错误日志并返回 None。然后，它将从 `txt_file` 中读取文件夹名称，
并对名称进行清理处理，移除任何可能对文件系统造成问题的字符。在创建文件夹时，函数会检查完整的文件夹路径长度是否超过 Windows 的路径长度限制，
如果超过，将会跳过此文件夹的创建并记录错误日志。

最后，函数返回一个列表，包含成功创建的所有文件夹的名称。如果在执行过程中发生任何错误或异常，函数将记录错误日志并返回 None。

在调试和排查问题时，可以参考此文件生成的日志信息。

函数的典型用法如下：

```python
folder_names = create_folders_batch("/target/directory/path", "/path/to/txt_file")
if folder_names:
    for name in folder_names:
        print(name)
```

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393。保留所有权利。
"""
import logging
from pathlib import Path
from typing import Union, Optional, List

from my_module import read_file_to_list
from my_module import sanitize_filename

logger = logging.getLogger(__name__)
MAX_PATH_LENGTH = 260


def create_folders_batch(target_path: Union[str, Path], txt_file: Union[str, Path]) -> Optional[List[str]]:
    """
    批量创建文件夹的函数。

    :param txt_file: 包含文件夹名称的文件路径
    :type txt_file: Union[str, Path]
    :param target_path: 目标目录
    :type target_path: Union[str, Path]
    :return: 创建的文件夹名称列表，如果过程中有错误发生，返回 None。
    :rtype: Optional[List[str]]
    """
    target_directory_path = Path(target_path)

    if not target_directory_path.exists():
        logger.error(f"The target directory {target_path} does not exist.")
        return None

    name_list = read_file_to_list(txt_file)

    if not name_list:
        logger.error(f"The list of folder names read from the file {txt_file} is empty.")
        return None

    name_list = [sanitize_filename(i) for i in name_list]
    successfully_created = []

    for name in name_list:
        folder_path = target_directory_path / name
        if len(str(folder_path)) > MAX_PATH_LENGTH:
            logger.error(f"The length of the folder path {folder_path} exceeds the maximum supported length in Windows.")
            continue
        try:
            folder_path.mkdir(parents=True, exist_ok=True)
            successfully_created.append(name)
        except Exception as e:
            logger.error(f"An error occurred while creating the folder {folder_path}. Error message: {str(e)}")

    return successfully_created if successfully_created else None
