"""
这个Python文件包含了一个主要的 sort_ru 函数和几个辅助函数（如 ru_login, ru_search 和 dir_move）。此文件主要用于根据特定的规则对目录中的文件进行排序和移动。

sort_ru 函数的主要功能是将源目录下的文件根据特定的规则排序，然后移动到目标目录。它首先获取源目录中的所有文件名，然后使用 ThreadPoolExecutor 来并行处理每个文件。对于每个文件，ru_search 函数会在已登录的会话中搜索该文件名，然后 dir_move 函数根据搜索结果将源文件移动到相应的目标目录。最后，如果目标目录下的 '0' 子目录中有文件，将会创建镜像。

ru_login 函数主要用于登录账户，并返回已登录的会话。

ru_search 函数用于在给定的会话中搜索指定的名字，并返回搜索结果。

dir_move 函数根据给定的搜索结果，将源目录下的特定文件移动到目标目录。

此模块主要用于文件的排序和移动操作，依赖于requests, retrying和concurrent.futures等第三方库，同时还引用了自定义的模块my_module。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""

import logging
import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple, Optional

import requests
from retrying import retry

from my_module import create_directories, read_json_to_dict

logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

CONFIG = read_json_to_dict('config/sort_ru.json')  # 配置文件

THREAD_NUMBER = CONFIG['sort_ru']['thread_number']  # 线程数
URL_SEARCH = CONFIG['sort_ru']['url_search']  # 搜索地址
USER_COOKIE = CONFIG['sort_ru']['user_cookie']  # 用户甜甜
REQUEST_HEAD = CONFIG['sort_ru']['request_head']  # 请求头

REQUEST_HEAD["Cookie"] = USER_COOKIE  # 请求头加入认证


def sort_ru(source_path: str, target_path: str) -> Dict[str, str]:
    """
    这个函数的主要功能是将源目录下的文件根据特定的规则排序，移动到目标目录。

    :param source_path: 源目录路径
    :type source_path: str
    :param target_path: 目标目录路径
    :type target_path: str
    :rtype: Dict[str, str]
    :return: 返回字典，包含源文件和目标文件的路径映射
    """
    final_path_dict = {}
    name_list = os.listdir(source_path)

    with ThreadPoolExecutor(max_workers=THREAD_NUMBER) as executor:
        # 提交每个任务并获取Future对象
        futures = [executor.submit(ru_search, name) for name in name_list]

        # 在每个任务完成时获取和处理结果
        for future in as_completed(futures):
            response = future.result()
            if response is not None:
                return_list = dir_move(source_path, target_path, response)
                final_path_dict.update(return_list)

    # 对特定目录建立镜像
    if len(os.listdir(os.path.join(target_path, '0'))) > 0:
        create_directories([os.path.join(target_path, 'Mirror', name) for name in os.listdir(os.path.join(target_path, '0'))])
        os.renames(os.path.join(target_path, '0'), os.path.join(target_path, 'done'))
        os.mkdir(os.path.join(target_path, '0'))

    return final_path_dict


@retry(stop_max_attempt_number=30, wait_random_min=30, wait_random_max=300)
def ru_search(name: str) -> Optional[Tuple[str, int]]:
    """
    通过session搜索名字，并返回搜索结果。不加入错误处理，让出错后自动重试。

    :param name: 搜索的名字
    :type name: str
    :rtype: Optional[Tuple[str, int]]
    :return: 返回一个元组，包含名字和搜索结果的数量
    """
    data = {"nm": name}
    response = requests.post(url=URL_SEARCH, headers=REQUEST_HEAD, data=data, timeout=30, verify=False, allow_redirects=True)
    return name, int(re.search(r'Результатов поиска: (\d+)', response.text).group(1))


def dir_move(source_path: str, target_path: str, response: Tuple[str, int]) -> Dict[str, str]:
    """
    根据搜索结果将源目录下的文件移动到目标目录。

    :param source_path: 源目录路径
    :type source_path: str
    :param target_path: 目标目录路径
    :type target_path: str
    :param response: 包含名字和搜索结果的元组
    :type response: Tuple[str, int]
    :rtype: Dict[str, str]
    :return: 返回字典，包含源文件和目标文件的路径映射
    """
    name, result = response
    source = os.path.join(source_path, name)
    if result >= 500:
        target = os.path.join(target_path, '500')
    elif 50 <= result < 500:
        target = os.path.join(target_path, '100')
    elif 0 < result <= 50:
        target = os.path.join(target_path, '50')
    elif result == 0:
        target = os.path.join(target_path, '0')
    else:
        logger.error(f'没有获取到搜索结果：{source}')
        return {}
    os.makedirs(target, exist_ok=True)
    shutil.move(source, target)
    logger.info(f"{name} 移动到 {target}")
    return {source: target}
