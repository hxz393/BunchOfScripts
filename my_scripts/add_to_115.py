"""
批量添加种子到 115 离线

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import logging
import os
from pathlib import Path
from typing import Any

from p115client import P115Client
from retrying import retry

from extract_torrent_download_link import extract_torrent_download_link
from my_module import read_json_to_dict

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict("config/add_to_115.json")
COOKIE_115 = CONFIG["cookie_115"]
MAGNET_PATH = CONFIG["magnet_path"]
MAX_TASK_NAME_LENGTH = 100


@retry(stop_max_attempt_number=3, wait_random_min=100, wait_random_max=1200)
def submit_offline_task(client: P115Client, download_link: str, save_path: str) -> Any:
    """提交单个离线任务。重试仅作用于单次提交。"""
    return client.offline_add_url(
        {
            "url": download_link,
            "savepath": save_path,
        }
    )


def add_to_115(source: str) -> None:
    """
    获取 JSON 或 LOG 文件中的下载链接，添加到 115 离线

    :param source: 来源目录
    :return: 无
    """
    source_path = Path(source)
    if not source_path.is_dir():
        logger.error(f"来源目录不存在: {source_path}")
        return

    client = P115Client(COOKIE_115)

    done = 0
    failed = 0
    skipped = 0
    for root, _, files in os.walk(source):
        root_path = Path(root)
        for file_name in files:
            file_path = root_path / file_name
            if file_path.suffix.lower() not in {".json", ".log"}:
                continue

            download_link = extract_torrent_download_link(file_path, MAGNET_PATH)
            if not download_link:
                skipped += 1
                continue

            task_name = file_path.stem[:MAX_TASK_NAME_LENGTH]
            save_path = f"{file_path.parent.name}/{task_name}"
            try:
                response = submit_offline_task(client, download_link, save_path)
            except Exception:
                logger.exception(f"115 离线添加失败: file={file_path}, savepath={save_path}")
                failed += 1
                continue

            logger.info(f"115 离线添加结果: file={file_path}, savepath={save_path}, resp={response}")
            done += 1

    logger.info(f"共添加 {done} 个任务，失败 {failed} 个，跳过 {skipped} 个无效文件。")
