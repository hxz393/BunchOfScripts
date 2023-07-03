"""
此 Python 文件包含一个名为 `move_duplicates` 的函数，用于在目录中寻找重复的文件夹，并将其移动到另一个目录。

`move_duplicates` 函数接收两个参数，`source_path` 和 `target_path`。`source_path` 是包含可能重复文件夹的源目录，
而 `target_path` 是重复文件夹移动的目标目录。

函数首先会检查 `source_path` 和 `target_path` 的有效性，如果无效，则记录错误日志并返回 None。然后，它将在 `source_path` 中寻找具有相似名称的文件夹。
这些文件夹的名称通过全局变量 `SEPARATORS` 中定义的分隔符分割，得到新文件夹名列表。如果新文件夹名列表中文件名在 `source_path` 中存在，函数会将其移动到 `target_path` 中。

最后，函数返回一个字典，字典的键是原始文件夹的路径，值是文件夹移动后的新路径。如果在执行过程中发生任何错误或异常，函数将记录错误日志并返回 None。

在调试和排查问题时，可以参考此文件生成的日志信息。

函数的典型用法如下：

```python
duplicates_moved = move_duplicates("/source/directory/path", "/target/directory/path")
if duplicates_moved:
    for original_path, new_path in duplicates_moved.items():
        print(f"Moved '{original_path}' to '{new_path}'")
```

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393。保留所有权利。
"""
import logging
import os
import shutil
from typing import Dict, Union, Optional

logger = logging.getLogger(__name__)

SEPARATORS = [" feat. ", " feat.", " feat ", " pres. ", " feating ",
              " featuring ", " b2b ", " ft ", " ft. ", " vs. ",
              " vs ", "⁄", " x ", "(1)"]


def move_duplicates(source_path: Union[str, os.PathLike], target_path: Union[str, os.PathLike]) -> Optional[Dict[str, str]]:
    """
    检查并移动源目录中的重复文件夹。

    :param source_path: 需要检查重复文件夹的源目录路径。
    :type source_path: Union[str, os.PathLike]
    :param target_path: 发现重复文件夹后，将其移动到的目标目录路径。
    :type target_path: Union[str, os.PathLike]
    :return: 一个字典，键为原文件夹路径，值为移动后的新文件夹路径，如果过程中有错误发生，返回 None。
    :rtype: Optional[Dict[str, str]]
    """
    if not os.path.exists(source_path):
        logger.error(f"Source directory '{source_path}' does not exist.")
        return None
    if not os.path.isdir(source_path):
        logger.error(f"'{source_path}' is not a valid directory.")
        return None
    if not os.path.exists(target_path):
        logger.error(f"Target directory '{target_path}' does not exist.")
        return None
    if not os.path.isdir(target_path):
        logger.error(f"'{target_path}' is not a valid directory.")
        return None

    file_dict = {i.lower(): os.path.normpath(os.path.join(source_path, i)) for i in os.listdir(source_path)}
    final_path_dict = {}

    for file_name, file_path in file_dict.items():
        split_word_list = [i for separator in SEPARATORS if separator in file_name for i in file_name.split(separator)]
        if not split_word_list:
            continue
        split_words_in_file_dict = [word for word in split_word_list if word.strip() in file_dict]
        if not split_words_in_file_dict:
            continue
        new_target_path = os.path.join(target_path, split_words_in_file_dict[0].strip())
        if os.path.exists(new_target_path):
            logger.error(f"'{file_path}' move skipped. The target '{new_target_path}' is exist")
            continue
        try:
            shutil.move(file_path, new_target_path)
            final_path_dict[file_path] = new_target_path
        except Exception as e:
            logger.error(f"An error occurred while moving the folder '{file_path}': {e}")

    return final_path_dict if final_path_dict else None
