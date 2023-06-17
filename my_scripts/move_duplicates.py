import os
import shutil
from typing import Dict, Union

SEPARATORS = [" feat. ", " feat.", " feat ", " pres. ", " feating ",
              " featuring ", " b2b ", " ft ", " ft. ", " vs. ",
              " vs ", "⁄", " x ", "(1)"]


def move_duplicates(source_path: Union[str, os.PathLike], target_path: Union[str, os.PathLike]) -> Dict[str, str]:
    """
    检查并移动源目录中的重复文件夹。

    :param source_path: 需要检查重复文件夹的源目录路径。
    :type source_path: Union[str, os.PathLike]
    :param target_path: 发现重复文件夹后，将其移动到的目标目录路径。
    :type target_path: Union[str, os.PathLike]
    :rtype: Dict[str, str]
    :return: 一个字典，键为原文件夹路径，值为移动后的新文件夹路径。
    :raise FileNotFoundError: 如果源目录或目标目录不存在，抛出 FileNotFoundError。
    :raise NotADirectoryError: 如果源目录或目标目录不是一个有效的目录，抛出 NotADirectoryError。
    :raise Exception: 如果在移动文件夹过程中出现其他问题，抛出 Exception。
    """

    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source directory '{source_path}' does not exist.")
    if not os.path.isdir(source_path):
        raise NotADirectoryError(f"'{source_path}' is not a valid directory.")
    if not os.path.exists(target_path):
        raise FileNotFoundError(f"Target directory '{target_path}' does not exist.")
    if not os.path.isdir(target_path):
        raise NotADirectoryError(f"'{target_path}' is not a valid directory.")

    file_dict = {str.lower(i): os.path.join(source_path, i) for i in os.listdir(source_path)}
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
            continue
        try:
            shutil.move(file_path, new_target_path)
            final_path_dict[file_path] = new_target_path
        except Exception as e:
            raise Exception(f"An error occurred while moving the folder: {e}")

    return final_path_dict


if __name__ == "__main__":
    source_path = r"resources/2"
    target_path = r"resources"
    try:
        final_path_dict = move_duplicates(source_path=source_path, target_path=target_path)
        print(final_path_dict)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
