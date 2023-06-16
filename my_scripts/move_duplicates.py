import os
import shutil

# 用于文件名分隔的字符列表
SEPARATORS = [" feat. ", " feat.", " feat ", " pres. ", " feating ",
              " featuring ", " b2b ", " ft ", " ft. ", " vs. ",
              " vs ", "⁄", " x ", "(1)"]

def move_duplicates(source_dir: str, target_dir: str) -> None:
    """
    检查并移动重复文件夹。

    :param source_dir: 要检查重复文件夹的源文件夹路径。
    :param target_dir: 如果发现重复文件夹，将其移动到的目标文件夹路径。
    :raise IOError: 如果源文件夹或目标文件夹不存在，将引发 IOError。
    :raise Exception: 如果移动文件夹过程中出现问题，将引发 Exception。
    :return: 无返回值
    """
    # 验证源文件夹和目标文件夹是否存在。如果不存在，抛出 IOError。
    if not os.path.exists(source_dir):
        raise IOError(f"源文件夹不存在：{source_dir}")
    if not os.path.exists(target_dir):
        raise IOError(f"目标文件夹不存在：{target_dir}")

    # 构建文件字典，其中键为文件名（转换为小写）和值为文件的完整路径。
    file_dict = {str.lower(i): os.path.join(source_dir, i) for i in os.listdir(source_dir)}

    for file_name, file_path in file_dict.items():
        # 使用预定义的分隔符列表将文件名分割为多个部分。如果分割后的部分列表非空，说明可能有重复的文件
        split_word_list = [i for separator in SEPARATORS if separator in file_name for i in file_name.split(separator)]
        if not split_word_list:
            continue
        # 如果分割的部分在文件字典中，说明有重复的文件
        split_words_in_file_dict = [word for word in split_word_list if word.strip() in file_dict]
        if not split_words_in_file_dict:
            continue
        # 拼凑然后检查目标目录是否存在，存在则不移动
        target_path = os.path.join(target_dir, split_words_in_file_dict[0].strip())
        if os.path.exists(target_path):
            print(f'无法移动 {file_path} 到 {target_path}，目标已存在')
            continue
        # 目标目录不存在时，尝试移动文件夹
        try:
            shutil.move(file_path, target_path)
            print(f'{file_path} 移动到 {target_path}')
        except Exception as e:
            raise Exception(f"移动文件夹时出错：{e}")


if __name__ == "__main__":
    try:
        move_duplicates(r"resources/2", r"resources")
    except Exception as e:
        print(f"运行时错误：{str(e)}")
