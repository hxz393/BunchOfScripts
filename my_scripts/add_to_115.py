"""
批量添加种子到 115 离线

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from p115client import P115Client
from retrying import retry

from my_module import read_json_to_dict, read_file_to_list

logger = logging.getLogger(__name__)

CONFIG_PATH = Path("config/add_to_115.json")
SUPPORTED_SUFFIXES = {".json", ".log"}
MAX_TASK_NAME_LENGTH = 100


def load_config() -> Tuple[str, str]:
    """读取并校验配置。"""
    config = read_json_to_dict(CONFIG_PATH)
    if not config:
        raise ValueError(f"读取配置失败: {CONFIG_PATH}")

    cookie_115 = config.get("cookie_115")
    magnet_path = config.get("magnet_path")
    if not cookie_115 or not magnet_path:
        raise KeyError(f"配置缺少必要字段: {CONFIG_PATH}")

    return cookie_115, magnet_path


def iter_source_files(source: str) -> Iterator[Path]:
    """遍历来源目录中的可处理文件。"""
    for root, _, files in os.walk(source):
        root_path = Path(root)
        for file_name in files:
            file_path = root_path / file_name
            if file_path.suffix.lower() in SUPPORTED_SUFFIXES:
                yield file_path


def build_save_path(file_path: Path) -> str:
    """根据文件名生成 115 保存目录。"""
    task_name = file_path.stem[:MAX_TASK_NAME_LENGTH]
    return f"{file_path.parent.name}/{task_name}"


def filter_torrents(torrents: List[Dict[str, Any]], key: str, priority_list: List[str]) -> List[Dict[str, Any]]:
    """按优先级过滤种子，如果出现意外字段值则抛错。"""
    unique_values = {torrent[key] for torrent in torrents}
    unexpected_values = unique_values - set(priority_list)
    if unexpected_values:
        raise ValueError(f"Unexpected value for {key}: {unexpected_values}")

    for value in priority_list:
        filtered = [torrent for torrent in torrents if torrent[key] == value]
        if filtered:
            return filtered
    return torrents


def select_best_yts_magnet(json_data: Dict[str, Any], magnet_path: str) -> str:
    """从 yts JSON 中选择最佳种子并生成磁链。"""
    torrents = json_data["data"]["movie"]["torrents"]
    torrents = filter_torrents(torrents, "quality", ["2160p", "1080p", "720p", "480p", "3D"])
    if len(torrents) == 1:
        return f"{magnet_path}{torrents[0]['hash']}"

    torrents = filter_torrents(torrents, "video_codec", ["x265", "x264"])
    if len(torrents) == 1:
        return f"{magnet_path}{torrents[0]['hash']}"

    torrents = filter_torrents(torrents, "bit_depth", ["10", "8"])
    if len(torrents) == 1:
        return f"{magnet_path}{torrents[0]['hash']}"

    torrents = filter_torrents(torrents, "type", ["bluray", "web"])
    if len(torrents) == 1:
        return f"{magnet_path}{torrents[0]['hash']}"

    best_torrent = max(torrents, key=lambda torrent: torrent["size_bytes"])
    return f"{magnet_path}{best_torrent['hash']}"


def extract_download_link(file_path: Path, magnet_path: str) -> Optional[str]:
    """从 JSON 或 LOG 文件中提取下载链接。"""
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

    lines = read_file_to_list(file_path)
    if not lines:
        logger.error(f"读取 LOG 失败或内容为空: {file_path}")
        return None
    return lines[0].lstrip("\ufeff")


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

    cookie_115, magnet_path = load_config()
    client = P115Client(cookie_115)

    done = 0
    failed = 0
    skipped = 0
    for file_path in iter_source_files(source):
        download_link = extract_download_link(file_path, magnet_path)
        if not download_link:
            skipped += 1
            continue

        save_path = build_save_path(file_path)
        try:
            response = submit_offline_task(client, download_link, save_path)
        except Exception:
            logger.exception(f"115 离线添加失败: file={file_path}, savepath={save_path}")
            failed += 1
            continue

        logger.info(f"115 离线添加结果: file={file_path}, savepath={save_path}, resp={response}")
        done += 1

    logger.info(f"共添加 {done} 个任务，失败 {failed} 个，跳过 {skipped} 个无效文件。")
