"""
这是一个Python文件，包含一个函数：`clean_input`。

`clean_input`函数的目标是处理用户输入，去除空行和重复行。如果输入字符串在处理过程中出现错误，函数会返回 None。函数接受一个参数：
- `input_str`：待处理的用户输入字符串。

此文件依赖于以下Python库：
- `logging`

函数使用了日志记录器记录任何在处理输入过程中发生的错误。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import logging

from typing import Optional

logger = logging.getLogger(__name__)


def clean_input(input_str: str) -> Optional[str]:
    """
    处理用户输入，去除空行和重复行后返回。

    :param input_str: 待处理的用户输入字符串
    :type input_str: str
    :rtype: Optional[str]
    :return: 去除空行和重复行后的字符串，或者在发生错误时返回 None。
    """
    try:
        # 分割成行
        lines = input_str.split("\n")

        # 移除空行和重复行
        seen = set()
        lines = [line for line in lines if line.strip() != "" and not (line in seen or seen.add(line))]

        # 将剩下的行重新组合成一个字符串
        output_str = "\n".join(lines)

        return output_str
    except Exception:
        logger.exception(f"An error occurred while processing the input")
        return None
