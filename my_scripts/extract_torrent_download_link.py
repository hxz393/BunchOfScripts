"""
从 JSON / LOG 来源文件中提取下载链接。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393。保留所有权利。
"""
import logging
import os
from pathlib import Path
from typing import Optional, Union

from scrapy_yts import select_best_yts_magnet

from my_module import read_file_to_list, read_json_to_dict

logger = logging.getLogger(__name__)


def extract_torrent_download_link(target_path: Union[str, os.PathLike], magnet_path: str) -> Optional[str]:
    """
    从来源文件中提取下载链接。

    目前支持：
    - ``.json``: YTS 电影详情 JSON，返回根据优先级挑选出的 magnet
    - ``.log``: 纯文本链接文件，返回首行去 BOM 后的链接

    :param target_path: 来源文件路径
    :param magnet_path: 生成 magnet 时使用的前缀
    :return: 提取到的下载链接；失败时返回 ``None``
    """
    file_path = Path(target_path)

    if file_path.suffix.lower() == ".json":
        json_data = read_json_to_dict(file_path)
        if not json_data:
            logger.error(f"读取 JSON 失败: {file_path}")
            return None
        try:
            return select_best_yts_magnet(json_data, magnet_path)
        except Exception:
            logger.exception(f"从 JSON 提取下载链接失败: {file_path}")
            return None

    if file_path.suffix.lower() == ".log":
        lines = read_file_to_list(file_path)
        if not lines:
            logger.error(f"读取 LOG 失败或内容为空: {file_path}")
            return None
        return lines[0].lstrip("\ufeff")

    return None
