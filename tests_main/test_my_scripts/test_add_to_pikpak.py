"""
针对 ``my_scripts.add_to_pikpak`` 的单元测试。

这些测试不依赖真实的 PikPak CLI、真实配置文件或真实网络。
重点验证：
1. 模块级配置读取。
2. 主流程对共享下载链接 helper 的调用。
3. PikPak CLI 参数构造、目录创建与主流程容错。
"""

import importlib.util
import subprocess
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import call, patch

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "add_to_pikpak.py"


def load_add_to_pikpak(config: dict | None = None):
    """在隔离环境中加载 ``add_to_pikpak`` 模块。"""
    module_config = {
        "pikpak_path": r"D:\Software\Program\pikpakcli\pikpakcli.exe",
        "pikpak_config": r"D:\Software\Program\pikpakcli\config.yml",
        "magnet_path": "magnet:?xt=urn:btih:",
    }
    if config:
        module_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: dict(module_config)

    fake_sort_movie_ops = types.ModuleType("sort_movie_ops")
    fake_sort_movie_ops.extract_torrent_download_link = lambda _path, _magnet_path: None

    spec = importlib.util.spec_from_file_location(
        f"add_to_pikpak_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "sort_movie_ops": fake_sort_movie_ops,
        },
    ):
        spec.loader.exec_module(module)

    return module


class TestConfig(unittest.TestCase):
    def test_module_constants_are_loaded_from_config(self):
        module = load_add_to_pikpak(
            {
                "pikpak_path": "pikpak.exe",
                "pikpak_config": "config.yml",
                "magnet_path": "prefix:",
            }
        )

        self.assertEqual(module.PIKPAK_PATH, "pikpak.exe")
        self.assertEqual(module.PIKPAK_CONFIG, "config.yml")
        self.assertEqual(module.MAGNET_PATH, "prefix:")


class TestHelpers(unittest.TestCase):
    def test_run_pikpak_command_uses_argv_list_and_shell_false(self):
        module = load_add_to_pikpak()
        completed = subprocess.CompletedProcess(args=["pikpak"], returncode=0, stdout="ok", stderr="")

        with patch.object(module.subprocess, "run", return_value=completed) as mock_run:
            result = module.run_pikpak_command("pikpak.exe", "config.yml", ["new", "folder", "-p", "/Director"])

        self.assertIs(result, completed)
        mock_run.assert_called_once_with(
            ["pikpak.exe", "--config", "config.yml", "new", "folder", "-p", "/Director"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            shell=False,
        )

    def test_create_pikpak_folder_skips_root(self):
        module = load_add_to_pikpak()

        with patch.object(module, "run_pikpak_command") as mock_run:
            self.assertTrue(module.create_pikpak_folder("/"))

        mock_run.assert_not_called()

    def test_create_pikpak_folder_treats_existing_folder_as_success(self):
        module = load_add_to_pikpak()
        completed = subprocess.CompletedProcess(args=["pikpak"], returncode=1, stdout="", stderr="folder already exists")

        with patch.object(module, "run_pikpak_command", return_value=completed) as mock_run:
            self.assertTrue(module.create_pikpak_folder("/Director"))

        mock_run.assert_called_once_with(
            module.PIKPAK_PATH,
            module.PIKPAK_CONFIG,
            ["new", "folder", "-p", "/Director"],
        )

    def test_add_pikpak_url_returns_false_on_failure(self):
        module = load_add_to_pikpak()
        completed = subprocess.CompletedProcess(args=["pikpak"], returncode=1, stdout="", stderr="quota exceeded")

        with patch.object(module, "run_pikpak_command", return_value=completed) as mock_run:
            self.assertFalse(module.add_pikpak_url("/Director", "movie", "magnet:?xt=urn:btih:HASH"))

        mock_run.assert_called_once_with(
            module.PIKPAK_PATH,
            module.PIKPAK_CONFIG,
            ["new", "url", "-p", "/Director", "-n", "movie", "-i", "magnet:?xt=urn:btih:HASH"],
        )


class TestAddToPikPakMain(unittest.TestCase):
    def test_add_to_pikpak_submits_valid_files_and_skips_invalid_entries(self):
        module = load_add_to_pikpak()

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "Director"
            root.mkdir()
            (root / "movie.json").write_text("{}", encoding="utf-8")
            (root / "movie.log").write_text("placeholder", encoding="utf-8")
            (root / "empty.log").write_text("", encoding="utf-8")
            (root / "ignore.txt").write_text("ignore", encoding="utf-8")

            def fake_extract(file_path, magnet_path):
                self.assertEqual(magnet_path, module.MAGNET_PATH)
                mapping = {
                    "movie.json": "magnet:?xt=urn:btih:JSON_HASH",
                    "movie.log": "magnet:?xt=urn:btih:LOG_HASH",
                    "empty.log": None,
                }
                return mapping[Path(file_path).name]

            with patch.object(module, "extract_torrent_download_link", side_effect=fake_extract) as mock_extract, \
                    patch.object(module, "create_pikpak_folder", return_value=True) as mock_create_folder, \
                    patch.object(module, "add_pikpak_url", return_value=True) as mock_add_url, \
                    patch.object(module.logger, "error"):
                module.add_to_pikpak(temp_dir)

        self.assertEqual(mock_extract.call_count, 3)
        mock_create_folder.assert_called_once_with("/Director")
        self.assertEqual(
            mock_add_url.call_args_list,
            [
                call("/Director", "movie", "magnet:?xt=urn:btih:JSON_HASH"),
                call("/Director", "movie", "magnet:?xt=urn:btih:LOG_HASH"),
            ],
        )

    def test_add_to_pikpak_continues_after_single_submit_failure(self):
        module = load_add_to_pikpak()

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "Director"
            root.mkdir()
            (root / "first.log").write_text("placeholder", encoding="utf-8")
            (root / "second.log").write_text("placeholder", encoding="utf-8")

            def fake_extract(file_path, _magnet_path):
                return f"magnet:?xt=urn:btih:{Path(file_path).stem.upper()}"

            with patch.object(module, "extract_torrent_download_link", side_effect=fake_extract), \
                    patch.object(module, "create_pikpak_folder", return_value=True) as mock_create_folder, \
                    patch.object(module, "add_pikpak_url", side_effect=[False, True]) as mock_add_url:
                module.add_to_pikpak(temp_dir)

        mock_create_folder.assert_called_once_with("/Director")
        self.assertEqual(mock_add_url.call_count, 2)


if __name__ == "__main__":
    unittest.main()
