"""
针对 ``my_scripts.add_to_115`` 的单元测试。

这些测试不依赖真实的 115 环境，也不会读取真实配置文件。
重点验证：
1. 模块级配置读取。
2. 主流程对共享下载链接 helper 的调用。
3. 主流程对跳过、失败继续和任务名截断的处理。
"""

import importlib.util
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, call, patch

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "add_to_115.py"


def load_add_to_115(config: dict | None = None):
    """在隔离环境中加载 ``add_to_115`` 模块。"""
    module_config = {
        "cookie_115": "cookie=value",
        "magnet_path": "magnet:?xt=urn:btih:",
    }
    if config:
        module_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: dict(module_config)
    fake_my_module.extract_torrent_download_link = lambda _path, _magnet_path: None

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    fake_p115client = types.ModuleType("p115client")

    class DummyP115Client:
        def __init__(self, cookie: str):
            self.cookie = cookie

        def offline_add_url(self, payload: dict):
            return {"ok": True, "payload": payload}

    fake_p115client.P115Client = DummyP115Client

    spec = importlib.util.spec_from_file_location(
        f"add_to_115_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "retrying": fake_retrying,
            "p115client": fake_p115client,
        },
    ):
        spec.loader.exec_module(module)

    return module


class TestConfig(unittest.TestCase):
    def test_module_constants_are_loaded_from_config(self):
        module = load_add_to_115(
            {
                "cookie_115": "cookie-a",
                "magnet_path": "prefix:",
            }
        )

        self.assertEqual(module.COOKIE_115, "cookie-a")
        self.assertEqual(module.MAGNET_PATH, "prefix:")


class TestAddTo115Main(unittest.TestCase):
    def test_add_to_115_submits_valid_files_and_skips_invalid_entries(self):
        module = load_add_to_115()

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "Director"
            root.mkdir()
            (root / "movie.json").write_text("{}", encoding="utf-8")
            (root / "movie.log").write_text("placeholder", encoding="utf-8")
            (root / "empty.log").write_text("", encoding="utf-8")
            (root / "ignore.txt").write_text("ignore", encoding="utf-8")

            client = Mock(name="p115_client")

            def fake_extract(file_path, magnet_path):
                self.assertEqual(magnet_path, module.MAGNET_PATH)
                mapping = {
                    "movie.json": "magnet:?xt=urn:btih:JSON_HASH",
                    "movie.log": "magnet:?xt=urn:btih:LOG_HASH",
                    "empty.log": None,
                }
                return mapping[Path(file_path).name]

            with patch.object(module, "extract_torrent_download_link", side_effect=fake_extract) as mock_extract, \
                    patch.object(module, "P115Client", return_value=client) as mock_client_cls, \
                    patch.object(module, "submit_offline_task", side_effect=[{"ok": True}, {"ok": True}]) as mock_submit:
                module.add_to_115(temp_dir)

        mock_client_cls.assert_called_once_with(module.COOKIE_115)
        self.assertEqual(mock_extract.call_count, 3)
        self.assertEqual(
            mock_submit.call_args_list,
            [
                call(client, "magnet:?xt=urn:btih:JSON_HASH", "Director/movie"),
                call(client, "magnet:?xt=urn:btih:LOG_HASH", "Director/movie"),
            ],
        )

    def test_add_to_115_truncates_long_task_name_in_main_flow(self):
        module = load_add_to_115()

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "Director"
            root.mkdir()
            long_name = "a" * 120
            file_path = root / f"{long_name}.log"
            file_path.write_text("placeholder", encoding="utf-8")

            client = Mock(name="p115_client")

            with patch.object(module, "extract_torrent_download_link", return_value="magnet:?xt=urn:btih:LOG_HASH"), \
                    patch.object(module, "P115Client", return_value=client), \
                    patch.object(module, "submit_offline_task", return_value={"ok": True}) as mock_submit:
                module.add_to_115(temp_dir)

        mock_submit.assert_called_once_with(client, "magnet:?xt=urn:btih:LOG_HASH", f"Director/{'a' * 100}")

    def test_add_to_115_continues_after_single_submit_failure(self):
        module = load_add_to_115()

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "Director"
            root.mkdir()
            (root / "first.log").write_text("placeholder", encoding="utf-8")
            (root / "second.log").write_text("placeholder", encoding="utf-8")

            client = Mock(name="p115_client")

            def fake_extract(file_path, _magnet_path):
                return f"magnet:?xt=urn:btih:{Path(file_path).stem.upper()}"

            with patch.object(module, "extract_torrent_download_link", side_effect=fake_extract), \
                    patch.object(module, "P115Client", return_value=client), \
                    patch.object(
                        module,
                        "submit_offline_task",
                        side_effect=[RuntimeError("boom"), {"ok": True}],
                    ) as mock_submit, \
                    patch.object(module.logger, "exception") as mock_log_exception:
                module.add_to_115(temp_dir)

        self.assertEqual(mock_submit.call_count, 2)
        mock_log_exception.assert_called_once()


if __name__ == "__main__":
    unittest.main()
