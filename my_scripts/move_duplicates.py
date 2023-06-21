import os
import shutil
import logging
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
