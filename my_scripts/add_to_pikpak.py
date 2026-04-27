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
from typing import List

from extract_torrent_download_link import extract_torrent_download_link
from my_module import read_json_to_dict

logger = logging.getLogger(__name__)

CONFIG = read_json_to_dict("config/add_to_pikpak.json")
PIKPAK_PATH = CONFIG["pikpak_path"]
PIKPAK_CONFIG = CONFIG["pikpak_config"]
MAGNET_PATH = CONFIG["magnet_path"]
MAX_TASK_NAME_LENGTH = 100


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


def create_pikpak_folder(remote_path: str) -> bool:
    """创建 PikPak 目录；根目录不需要创建。"""
    if remote_path == "/":
        return True

    result = run_pikpak_command(
        PIKPAK_PATH,
        PIKPAK_CONFIG,
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


def add_pikpak_url(remote_path: str, task_name: str, download_link: str) -> bool:
    """添加单个 PikPak 离线 URL 任务。"""
    result = run_pikpak_command(
        PIKPAK_PATH,
        PIKPAK_CONFIG,
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

    done = 0
    failed = 0
    skipped = 0
    folder_failed = 0
    attempted_paths = set()

    for root, _, files in os.walk(source):
        root_path = Path(root)
        for file_name in files:
            file_path = root_path / file_name
            if file_path.suffix.lower() not in {".json", ".log"}:
                continue

            relative_parent = file_path.parent.relative_to(source_path)
            remote_path = "/" if relative_parent == Path(".") else f"/{relative_parent.as_posix()}"
            if remote_path not in attempted_paths:
                attempted_paths.add(remote_path)
                if not create_pikpak_folder(remote_path):
                    folder_failed += 1

            download_link = extract_torrent_download_link(file_path, MAGNET_PATH)
            if not download_link:
                skipped += 1
                continue

            task_name = file_path.stem[:MAX_TASK_NAME_LENGTH]
            if add_pikpak_url(remote_path, task_name, download_link):
                done += 1
            else:
                failed += 1

    logger.info(f"共添加 {done} 个任务，失败 {failed} 个，跳过 {skipped} 个无效文件，目录失败 {folder_failed} 个。")
