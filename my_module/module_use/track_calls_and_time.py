"""
这是一个用于增强函数调试功能的Python模块。

此模块提供了一个装饰器 `track_calls_and_time`，用于跟踪函数的调用次数和执行时间。这对于性能调试和分析特别有用。

本模块的主要目的是提供一个简单的方式来监控和记录函数的性能表现。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""

import time
import logging
from functools import wraps

# 初始化日志记录器
logger = logging.getLogger(__name__)


def track_calls_and_time(func):
    """
    装饰器：跟踪函数调用次数和总运行时间。

    :param func: 要装饰的函数
    :type func: Callable
    :return: 装饰后的函数
    :rtype: Callable
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        """
        装饰器实现函数的调用次数和总运行时间。
        :param args: 是一个用于非关键字参数的可变参数列表。它允许函数接收任意数量的位置参数。
        :param kwargs: 是一个用于关键字参数的可变参数字典。它允许函数接收任意数量的关键字参数。
        """
        wrapper.calls += 1
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
        except Exception:
            logger.exception(f"Exception occurred in {func.__name__}")
            return None
        end_time = time.time()
        wrapper.total_time += (end_time - start_time)
        logger.debug(f"{func.__name__} called {wrapper.calls} times, total run time: {wrapper.total_time:.4f} seconds")
        print(f"{func.__name__} called {wrapper.calls} times, total run time: {wrapper.total_time:.4f} seconds")
        return result

    wrapper.calls = 0
    wrapper.total_time = 0
    return wrapper

if __name__ == '__main__':
    # 示例使用
    @track_calls_and_time
    def example_function(x):
        """
        示例函数
        :param x:
        :return:
        """
        return x * x


    for i in range(5):
        example_function(i)