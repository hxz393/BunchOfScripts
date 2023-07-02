import logging
import os
import signal
import subprocess
from typing import Optional

from mitmproxy import http

logger = logging.getLogger(__name__)


def request(flow: http.HTTPFlow) -> None:
    """
    修改 HTTP 请求中的 'Referer' 字段。

    :type flow: http.HTTPFlow
    :param flow: mitmproxy 的 HTTPFlow 对象，包含了 HTTP 请求和响应的信息。
    """
    flow.request.headers["Referer"] = "http://jandan.net/"


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
    except Exception as e:
        logger.error("An error occurred while starting mitmproxy: %s", str(e))
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


def jandan_net(script_path: str = "my_scripts\jandan_net.py") -> None:
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
    mitmproxy --set keep_alive=120 -p 10808 -s my_scripts/jandan_net.py
    '''
    main_process = run_mitmproxy(r"jandan_net.py")
    if main_process:
        monitor_process(main_process)
