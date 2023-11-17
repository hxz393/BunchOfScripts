"""
这是一个Python文件，包含一个函数：`list_to_str`。

`list_to_str`函数的目标是将输入列表中的元素转换为字符串并连接在一起，元素之间用换行符("\n")分隔。如果输入列表为空或者为None，则返回None。函数接受一个参数：
- `my_list`：输入的列表，其元素为字符串或整数。

此文件依赖于以下Python库：
- `typing`
- `logging`

函数使用了日志记录器记录任何在转换过程中发生的错误。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import logging
from typing import List, Union, Optional

logger = logging.getLogger(__name__)


def list_to_str(my_list: Optional[List[Union[str, int]]]) -> Optional[str]:
    """
    将列表中的元素转换为字符串并连接在一起。元素之间用换行符("\n")分隔。

    :type my_list: Optional[List[Union[str, int]]]
    :param my_list: 输入的列表，其元素为字符串或整数。如果列表为空或为 None，将返回 None。
    :rtype: Optional[str]
    :return: 一个字符串，其中包含列表中所有元素的字符串表示形式，元素之间用换行符分隔，或者在列表为空或为 None 时返回 None。
    """
    try:
        if my_list:
            return "\n".join(str(element) for element in my_list)
        else:
            return None
    except Exception:
        logger.exception(f"An error occurred while converting list to string")
        return None
