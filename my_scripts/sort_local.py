"""
这个Python文件包含两个函数：run_sort 和 sort_local。

函数 run_sort 的主要作用是移动满足特定条件的源文件到指定的目标目录。该函数接受四个参数：source_to_move、target_root_dir、camp_names 和 sept_list，分别代表需要移动的源文件路径、目标根目录、包含目标文件名的列表以及分隔符列表。它会判断文件名是否满足条件，如果满足则将文件移动到指定的目标目录，并返回一个字典，其中包含源文件和目标文件的路径。

函数 sort_local 的功能是分类本地文件并移动到相应的目标目录。该函数接受三个参数：source_path、target_path 和 comp_list，分别代表原始目录、目标目录和比较列表。在执行过程中，它会使用多进程的方式进行文件的移动，从而提高程序的运行效率。并且，这个函数会返回一个字典，其中包含所有被移动的文件的原始路径和目标路径。

本模块主要用于进行大规模的本地文件整理工作，包括文件分类以及文件的移动等操作。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2023, hxz393. 保留所有权利。
"""

import logging

import os
from multiprocessing import Pool
from shutil import move
from typing import List, Optional, Dict

from my_module import get_subdirectories, create_directories, read_json_to_dict

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict('config/sort_local.json')  # 配置文件
SOURCE_PATH = CONFIG['sort_local']['source_path']  # 原始目录
TARGET_PATH = CONFIG['sort_local']['target_path']  # 目标目录
COMP_LIST = CONFIG['sort_local']['comp_list']  # 比较列表
SEPT_LIST = CONFIG['sort_local']['sep_list']  # 符号列表
PROCESSES_NUMBER = CONFIG['sort_local']['processes_number']  # 进程数


def run_sort(source_to_move: str, target_root_dir: str, camp_names: List[str]) -> Optional[Dict[str, str]]:
    """
    将满足条件的源文件夹移动到目标目录，并返回单条字典，其中包含源文件和目标文件的路径。

    :param source_to_move: 需要移动的源文件路径。
    :type source_to_move: str
    :param target_root_dir: 目标根目录。
    :type target_root_dir: str
    :param camp_names: 包含目标文件名的列表。
    :type camp_names: List[str]
    :return: 如果文件移动成功，返回一个包含源文件和目标文件路径的字典；否则返回None。
    :rtype: Optional[Dict[str, str]]
    """

    try:
        source_org_name = os.path.basename(source_to_move)
        target_to_move = os.path.join(target_root_dir, source_org_name)

        source_fix = source_org_name.lower().replace('.', ' ').replace('  ', ' ').replace("'", '').strip()
        # print(source_fix)
        source_sept = [source_fix.split(sept)[0].strip().rstrip('.') for sept in SEPT_LIST if sept in source_fix]
        # print(source_sept)

        if source_fix in camp_names or any(sept in camp_names for sept in source_sept):
            move(source_to_move, target_to_move)
            logger.info(f'{source_to_move} 移动到：{target_to_move}')
            return {source_to_move: target_to_move}
        # else:
        #     print(source_fix)
        #     print(camp_names)
    except Exception:
        logger.exception("出错了")
        return None


def sort_local(source_path: str = SOURCE_PATH, target_path: str = TARGET_PATH) -> Dict[str, str]:
    """
    分类本地文件并移动到相应的目标目录，并返回最终字典，其中包含所有被移动的文件的原始路径和目标路径。

    :param source_path: 原始目录，默认为全局变量SOURCE_PATH。
    :type source_path: str
    :param target_path: 目标目录，默认为全局变量TARGET_PATH。
    :type target_path: str
    :return: 返回最终字典，其中包含所有被移动的文件的原始路径和目标路径。
    :rtype: Dict[str, str]
    """

    final_path_dict = {}
    if not os.path.isdir(source_path):
        logger.error(f"源目录不存在：{source_path}")
        return final_path_dict
    elif not os.path.isdir(target_path):
        logger.error(f"目标目录不存在：{target_path}")
        return final_path_dict

    try:
        comp_subdirs = {comp_dir: [name.lower().replace('.', ' ').replace('  ', ' ') for name in os.listdir(comp_dir)] for comp_dir in COMP_LIST}
        # print(comp_subdirs)

        for comp_dir, camp_names in comp_subdirs.items():
            target_dir_name = os.path.basename(comp_dir) if os.path.basename(comp_dir) != 'Mirror' else 'done'
            target_root_dir = os.path.join(target_path, target_dir_name)
            source_paths = get_subdirectories(source_path)

            with Pool(processes=PROCESSES_NUMBER) as pool:
                results = pool.starmap(run_sort, [(source_to_move, target_root_dir, camp_names) for source_to_move in source_paths])
                for result in results:
                    final_path_dict.update(result) if result is not None else None
            target_dirs = get_subdirectories(target_root_dir)
            if target_dir_name == 'done' and target_dirs:
                create_directories([path.replace('done', 'mirror') for path in target_dirs])
        return final_path_dict
    except Exception:
        logger.exception("出错了")
        return final_path_dict
