"""
批量添加种子到 pikpak 离线。使用第三方客户端：https://github.com/52funny/pikpakcli
配额查询：https://mypikpak.com/drive/all?action=show_traffic_dialog

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import logging
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from my_module import read_json_to_dict, read_file_to_list

logger = logging.getLogger(__name__)

CONFIG_PATH = Path("config/add_to_pikpak.json")
SUPPORTED_SUFFIXES = {".json", ".log"}
MAX_TASK_NAME_LENGTH = 100


def load_config() -> Tuple[str, str, str]:
    """读取并校验配置。"""
    config = read_json_to_dict(CONFIG_PATH)
    if not config:
        raise ValueError(f"读取配置失败: {CONFIG_PATH}")

    pikpak_path = config.get("pikpak_path")
    pikpak_config = config.get("pikpak_config")
    magnet_path = config.get("magnet_path")
    if not pikpak_path or not pikpak_config or not magnet_path:
        raise KeyError(f"配置缺少必要字段: {CONFIG_PATH}")

    return pikpak_path, pikpak_config, magnet_path


def iter_source_files(source: str) -> Iterator[Path]:
    """遍历来源目录中的可处理文件。"""
    for root, _, files in os.walk(source):
        root_path = Path(root)
        for file_name in files:
            file_path = root_path / file_name
            if file_path.suffix.lower() in SUPPORTED_SUFFIXES:
                yield file_path


def build_task_name(file_path: Path) -> str:
    """根据文件名生成 PikPak 任务名。"""
    return file_path.stem[:MAX_TASK_NAME_LENGTH]


def build_remote_path(source_root: Path, file_path: Path) -> str:
    """根据来源目录生成 PikPak 远程目录。"""
    relative_parent = file_path.parent.relative_to(source_root)
    if relative_parent == Path("."):
        return "/"
    return f"/{relative_parent.as_posix()}"


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


def run_pikpak_command(pikpak_path: str, pikpak_config: str, args: List[str]) -> subprocess.CompletedProcess:
    """执行 PikPak CLI 命令并捕获输出。"""
    command = [pikpak_path, "--config", pikpak_config, *args]
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        shell=False,
    )


def ensure_remote_folder(pikpak_path: str, pikpak_config: str, remote_path: str) -> bool:
    """确保远程目录存在。根目录不需要创建。"""
    if remote_path == "/":
        return True

    result = run_pikpak_command(
        pikpak_path,
        pikpak_config,
        ["new", "folder", "-p", remote_path],
    )
    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    combined_output = f"{stdout}\n{stderr}".lower()
    if result.returncode == 0 or "exist" in combined_output:
        logger.info(f"PikPak 目录已就绪: path={remote_path}, stdout={stdout}, stderr={stderr}")
        return True

    logger.error(
        f"PikPak 创建目录失败: path={remote_path}, returncode={result.returncode}, stdout={stdout}, stderr={stderr}"
    )
    return False


def submit_url(
        pikpak_path: str,
        pikpak_config: str,
        remote_path: str,
        task_name: str,
        download_link: str,
) -> bool:
    """提交单个 PikPak 离线任务。"""
    result = run_pikpak_command(
        pikpak_path,
        pikpak_config,
        ["new", "url", "-p", remote_path, "-n", task_name, "-i", download_link],
    )
    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    if result.returncode == 0:
        logger.info(f"PikPak 添加成功: path={remote_path}, name={task_name}, stdout={stdout}, stderr={stderr}")
        return True

    logger.error(
        f"PikPak 添加失败: path={remote_path}, name={task_name}, returncode={result.returncode}, stdout={stdout}, stderr={stderr}"
    )
    return False


def add_to_pikpak(source: str) -> None:
    """
    获取 JSON 或 LOG 文件中的下载链接，添加到 pikpak 离线

    :param source: 来源目录
    :return: 无
    """
    source_path = Path(source)
    if not source_path.is_dir():
        logger.error(f"来源目录不存在: {source_path}")
        return

    pikpak_path, pikpak_config, magnet_path = load_config()

    done = 0
    failed = 0
    skipped = 0
    folder_failed = 0
    attempted_paths = set()

    for file_path in iter_source_files(source):
        remote_path = build_remote_path(source_path, file_path)
        if remote_path not in attempted_paths:
            attempted_paths.add(remote_path)
            if not ensure_remote_folder(pikpak_path, pikpak_config, remote_path):
                folder_failed += 1

        download_link = extract_download_link(file_path, magnet_path)
        if not download_link:
            skipped += 1
            continue

        task_name = build_task_name(file_path)
        if submit_url(pikpak_path, pikpak_config, remote_path, task_name, download_link):
            done += 1
        else:
            failed += 1

    logger.info(f"共添加 {done} 个任务，失败 {failed} 个，跳过 {skipped} 个无效文件，目录失败 {folder_failed} 个。")
