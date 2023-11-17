"""
此 Python 文件主要用于控制和监控 mitmproxy，并修改 HTTP 请求的 'Referer' 字段。主要包含以下几个函数：

`request` 函数用于修改 HTTP 请求的 'Referer' 字段。

`run_mitmproxy` 函数用于启动 mitmproxy 并加载指定的 Python 脚本。

`monitor_process` 函数用于实时监控和打印 mitmproxy 的输出。

`scrapy_pic_1` 函数则是启动并监控 mitmproxy 的高级接口，用于执行特定的 Python 脚本。

主要使用方法如下：

首先，将需要执行的 Python 脚本文件的路径作为参数传递给 `run_mitmproxy` 函数。该函数将启动 mitmproxy，并加载该脚本。

然后，将 `run_mitmproxy` 返回的 Popen 对象传递给 `monitor_process` 函数。这个函数将实时监控 mitmproxy 的输出，并打印到终端。

你也可以直接使用 `scrapy_pic_1` 函数，只需传入 Python 脚本的路径即可。该函数将调用上述两个函数，并自动处理错误和异常。

注意：所有的路径都是相对于本 Python 文件所在目录的。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""

import logging
import os
import signal
import subprocess
from typing import Optional

from mitmproxy import http

from my_module import read_json_to_dict

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict('config/scrapy_pic_1.json')
BASE_URL = CONFIG['scrapy_pic_1']['base_url']  # 网站域名


def request(flow: http.HTTPFlow, base_url: str = BASE_URL) -> None:
    """
    修改 HTTP 请求中的 'Referer' 字段。

    :type base_url: str
    :param base_url: 网站域名
    :type flow: http.HTTPFlow
    :param flow: mitmproxy 的 HTTPFlow 对象，包含了 HTTP 请求和响应的信息。
    """
    flow.request.headers["Referer"] = base_url


def run_mitmproxy(script_path: str) -> Optional[subprocess.Popen]:
    """
    运行 mitmproxy 并使用指定的 Python 脚本。

    :type script_path: str
    :param script_path: 用于 mitmproxy 的 Python 脚本的路径。
    :rtype: Optional[subprocess.Popen]
    :return: 一个表示 mitmproxy 进程的 Popen 对象，或者在发生错误时返回 None。
    """
    try:
        # 将文件路径标准化，使其与操作系统的文件路径格式相匹配
        script_path = os.path.normpath(script_path)

        # 检查文件是否存在
        if not os.path.exists(script_path):
            logger.error("The script file does not exist: %s", script_path)
            return None

        # 定义 mitmproxy 的命令和参数
        cmd = f'mitmdump --set keep_alive=120 -p 10808 -s {script_path}'

        # 使用 subprocess 启动 mitmproxy 并捕获其输出
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        return process
    except Exception:
        logger.exception("启动报错: %s", str(e))
        return None


def monitor_process(process: subprocess.Popen) -> None:
    """
    监控并打印进程的输出。

    :type process: subprocess.Popen
    :param process: 一个 Popen 对象，表示要监控的进程。
    """
    try:
        while True:
            # 读取进程的输出
            output = process.stdout.readline()

            if output:
                print(output.decode(), end='')
            elif process.poll() is not None:
                break
    except KeyboardInterrupt:
        # 当用户按下 Ctrl+C 时，发送一个 SIGINT 信号给进程，让它正确地停止
        process.send_signal(signal.SIGINT)

        # 等待进程结束
        process.wait()


def scrapy_pic_1(script_path: str = "my_scripts/scrapy_pic_1.py") -> None:
    """
    启动并监控 mitmproxy。

    :type script_path: str
    :param script_path: 用于 mitmproxy 的 Python 脚本的路径。
    """
    process = run_mitmproxy(script_path)
    if process:
        monitor_process(process)


if __name__ == '__main__':
    '''
    在终端运行 
    mitmproxy --set keep_alive=120 -p 10808 -s my_scripts/scrapy_pic_1.py
    '''
    main_process = run_mitmproxy(r"scrapy_pic_1.py")
    if main_process:
        monitor_process(main_process)
