"""
这是一个Python文件，其中包含一个函数：`config_get`。

`config_get`函数的目标是从给定的配置解析器对象中，使用指定的getter函数来获取特定选项的值。如果getter函数抛出ValueError异常，那么函数会尝试从默认section获取该选项的值。

这个函数接受四个参数：
- `config`: 是配置解析器对象，类型为`configparser.ConfigParser`。
- `section`: 是配置中的section名称，类型为`str`。
- `option`: 是需要获取的选项名称，类型为`str`。
- `getter`: 是用于获取值的函数，类型为`Callable[[str, str], Any]`。

函数的返回值是从配置中获取到的值，类型可以为任意类型。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import configparser
from typing import Any, Callable


def config_get(config: configparser.ConfigParser, section: str, option: str, getter: Callable[[str, str], Any]) -> Any:
    """
    尝试使用指定的 getter 函数从配置中获取指定选项的值。
    如果 getter 函数引发 ValueError 异常，则尝试从 DEFAULT section 获取该选项的值。

    :param config: 配置解析器对象
    :type config: configparser.ConfigParser
    :param section: 配置中的 section 名称
    :type section: str
    :param option: 要获取的选项名称
    :type option: str
    :param getter: 用于获取值的函数
    :type getter: Callable[[str, str], Any]
    :return: 获取的配置值
    :rtype: Any
    """
    try:
        return getter(section, option)
    except ValueError:
        return getter(config.default_section, option)
