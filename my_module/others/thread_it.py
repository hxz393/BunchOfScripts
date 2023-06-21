import time
from typing import Any, Callable, Optional, Tuple
import logging
from threading import Thread

# 初始化日志记录器
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
    def wrapper(*args: Tuple[Any]) -> None:
        try:
            func(*args)
        except Exception as e:
            logger.error(f"Error occurred in thread {name}: {e}")

    t = Thread(target=wrapper, args=args, daemon=daemon, name=name)
    t.start()

if __name__ == '__main__':
    # 这是一个示例函数
    def example_func(x: int, y: int) -> int:
        print(x + y)
        return x + y
    thread_it(example_func, 1, 2, name='example_thread')


    # 这是一个需要较长时间完成的函数
    def long_running_task(duration: int, task_name: str) -> None:
        for i in range(duration):
            print(f"{task_name}: {i + 1}/{duration}")
            time.sleep(1)  # 等待一秒
        print(f"{task_name} finished.")

    # 启动新线程运行这个函数
    thread_it(long_running_task, 5, 'Task 1', name='TaskThread1')
    thread_it(long_running_task, 7, 'Task 2', name='TaskThread2')
    thread_it(long_running_task, 3, 'Task 3', name='TaskThread3')

    # 主线程继续执行
    for i in range(10):
        print(f"Main thread: {i + 1}/10")
        time.sleep(1)
    print("Main thread finished.")