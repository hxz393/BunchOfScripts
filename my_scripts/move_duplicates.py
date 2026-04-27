"""
按艺人目录名中的合作标记识别“重复候选”目录，并移动到单独目录。

这个脚本用于整理艺人目录。典型场景是源目录下同时存在：
- ``artist a``
- ``artist a feat. artist b``

当目录名包含 ``feat`` / ``vs`` / ``x`` / ``with`` 等合作分隔符，并且拆分出的某一侧
艺人名已经作为独立目录存在于同一层级时，当前目录会被视为重复候选并移动到目标目录。

匹配过程使用小写名做比较，因此大小写不会影响识别结果。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393。保留所有权利。
"""
import logging

import os
import shutil
from typing import Dict, Union, Optional

logger = logging.getLogger(__name__)

SEPARATORS = [" feat. ", " feat.", " feat ", " pres. ", " feating ",
              " featuring ", " b2b ", " ft ", " ft. ", " vs. ",
              " vs ", "⁄", " x ", "(1)", " with ", " (feat"]


def move_duplicates(source_path: Union[str, os.PathLike], target_path: Union[str, os.PathLike]) -> Optional[Dict[str, str]]:
    """
    检查源目录中的合作艺人目录，并把命中的重复候选移动到目标目录。

    这里的“重复候选”指目录名可被 ``SEPARATORS`` 中的合作分隔符拆开，
    且拆出的某个艺人名已经作为独立条目存在于 ``source_path`` 中。

    :param source_path: 需要检查的艺人目录根路径。
    :type source_path: Union[str, os.PathLike]
    :param target_path: 命中重复候选后，目录移动到的目标根路径。
    :type target_path: Union[str, os.PathLike]
    :return: 一个字典，键为原路径，值为移动后的新路径；如果没有移动任何目录则返回 None。
    :rtype: Optional[Dict[str, str]]
    """
    if not os.path.exists(source_path):
        logger.error(f"源目录不存在：{source_path}")
        return None
    if not os.path.isdir(source_path):
        logger.error(f"源目录不正确：{source_path}")
        return None
    if not os.path.exists(target_path):
        logger.error(f"目标目录不存在：{target_path}")
        return None
    if not os.path.isdir(target_path):
        logger.error(f"目标目录不正确：{target_path}")
        return None

    # 仅检查 source_path 第一层条目，匹配时统一转成小写，避免大小写差异影响识别。
    file_dict = {i.lower(): os.path.normpath(os.path.join(source_path, i)) for i in os.listdir(source_path)}
    final_path_dict = {}

    for file_name, file_path in file_dict.items():
        # 例如 "artist a feat. artist b" 会被拆成 ["artist a", "artist b"]。
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
            logger.info(f"{file_path} 移动到 {new_target_path}")
        except Exception:
            logger.exception(f"移动目录时发生错误：{file_path}")

    return final_path_dict if final_path_dict else None
