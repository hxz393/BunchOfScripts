"""
这是一个Python文件，提供一个函数 `thread_it`，它允许在新的线程中运行任何指定的函数。这对于处理需要并行处理或需要在后台运行的长时间运行任务非常有用。

函数接受以下参数：
- `func`：需要在新线程中运行的函数。
- `*args`：传递给函数的参数，可以是任意数量和类型的参数。
- `daemon`：是否为后台线程，默认为 True。
- `name`：新线程的名字，如果没有指定，则默认为 None。

此函数不返回任何值。如果在执行给定的函数时发生任何错误，该错误将被捕获并记录到日志中。

此文件也包含一个主程序，该程序演示了如何使用 `thread_it` 函数。主程序首先定义了两个示例函数，然后在新的线程中运行这些函数。在此期间，主线程继续执行其自己的任务。

此文件依赖于以下Python库：
- `time`
- `logging`
- `threading`

该文件使用日志记录器来记录在执行给定函数时可能发生的任何错误。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""
import logging
import traceback
from threading import Thread
from typing import Any, Callable, Optional, Tuple

logger = logging.getLogger(__name__)


def thread_it(func: Callable[..., Any], *args: Any, daemon: Optional[bool] = True, name: Optional[str] = None) -> None:
    """
    在新的线程中运行函数。

    :param func: 需要在新线程中运行的函数。
    :type func: Callable[..., Any]
    :param args: 函数的参数，可以是任意类型。
    :type args: Any
    :param daemon: 是否为后台线程，默认为 True。
    :type daemon: Optional[bool]
    :param name: 新线程的名字，如果没有指定，则默认为 None。
    :type name: Optional[str]
    :return: 无返回值
    :rtype: None
    :raise: 不会抛出异常，所有异常都被记录到日志中。
    """

    def wrapper(*arg: Tuple[Any]) -> None:
        """
        一个封装了给定函数的内部函数，用于在新线程中运行。它接收一个可变数量的参数并将其传递给给定的函数。 
        如果在执行给定函数时发生错误，该错误将被捕获并记录到日志中。

        :param arg: 传递给函数的参数，可以是任意类型和数量。
        :type arg: Tuple[Any]
        :return: 无返回值
        :rtype: None
        """
        try:
            func(*arg)
        except Exception as e:
            logger.error(f"Error occurred in thread {name}: {e}\n{traceback.format_exc()}")

    t = Thread(target=wrapper, args=args, daemon=daemon, name=name)
    t.start()
