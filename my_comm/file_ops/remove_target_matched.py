from typing import List, Union
from pathlib import Path

from my_comm.file_ops.remove_target import remove_target


def remove_target_matched(path: Union[str, Path], match_list: List[str]) -> None:
    """
    删除完全匹配目标列表中任一名字的文件或文件夹。

    :param path: 需要搜索的路径
    :type path: Union[str, Path]
    :param match_list: 需要匹配的目标列表
    :type match_list: List[str]
    :raise FileNotFoundError: 如果指定路径不存在会抛出此异常
    :raise ValueError: 如果匹配目标列表为空会抛出此异常
    """

    if not isinstance(path, Path):
        path = Path(path)

    # 检查路径是否存在
    if not path.exists():
        raise FileNotFoundError(f"指定路径 {path} 不存在.")

    # 检查匹配目标列表是否为空
    if not match_list:
        raise ValueError(f"匹配目标列表为空.")

    try:
        # 获取所有匹配的路径
        matched_paths = [p for p in path.glob('**/*') if p.name in match_list]

        # 批量删除匹配的目标
        for matched_path in matched_paths:
            remove_target(matched_path)
    except Exception as e:
        raise Exception(f"删除匹配目标时出错. 错误信息: {str(e)}")


if __name__ == "__main__":
    try:
        remove_target_matched(r'./resources', ['test.txt', 'a'])
    except Exception as e:
        print(str(e))
