"""
针对 ``my_scripts.get_qb_downloads`` 的定向单元测试。

这些测试不依赖真实 qBittorrent 服务或真实配置文件。
重点验证：
1. 登录成功后会把下载任务的磁链写到目标目录。
2. 登录失败或没有下载任务时会短路返回。
3. 获取下载任务时会请求正确的 QB API，并处理失败响应。
"""

import importlib.util
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "get_qb_downloads.py"


def load_get_qb_downloads(config: dict | None = None):
    """在隔离依赖的环境中加载 ``get_qb_downloads`` 模块。"""
    module_config = {
        "qb_url": "https://example.com/qb",
        "qb_save_dir": "/downloads/qb",
        "magnet_path": "output",
    }
    if config:
        module_config.update(config)

    fake_add_to_qb = types.ModuleType("add_to_qb")
    fake_add_to_qb.qb_login = lambda _session: True

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: dict(module_config)

    spec = importlib.util.spec_from_file_location(
        f"get_qb_downloads_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "add_to_qb": fake_add_to_qb,
            "my_module": fake_my_module,
        },
    ):
        spec.loader.exec_module(module)

    return module


class TestGetQbDownloadsMain(unittest.TestCase):
    def setUp(self):
        self.module = load_get_qb_downloads()

    def test_get_qb_downloads_writes_magnet_logs(self):
        torrents = [
            {
                "magnet_uri": "magnet:?xt=urn:btih:FIRST",
                "save_path": "/downloads/Alpha",
                "tags": "DirectorA",
            },
            {
                "magnet_uri": "magnet:?xt=urn:btih:SECOND",
                "save_path": "C:\\qb\\Beta",
                "tags": "DirectorB",
            },
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            fake_session = Mock(name="session")

            with patch.object(self.module.requests, "Session", return_value=fake_session), \
                    patch.object(self.module, "qb_login", return_value=True) as mock_login, \
                    patch.object(self.module, "get_qb_torrents", return_value=torrents) as mock_torrents:
                self.module.get_qb_downloads(temp_dir)

            mock_login.assert_called_once_with(fake_session)
            mock_torrents.assert_called_once_with(fake_session)
            self.assertEqual(
                (Path(temp_dir) / "DirectorA" / "Alpha.log").read_text(encoding="utf-8"),
                "magnet:?xt=urn:btih:FIRST",
            )
            self.assertEqual(
                (Path(temp_dir) / "DirectorB" / "Beta.log").read_text(encoding="utf-8"),
                "magnet:?xt=urn:btih:SECOND",
            )

    def test_get_qb_downloads_returns_early_when_login_fails(self):
        fake_session = Mock(name="session")

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(self.module.requests, "Session", return_value=fake_session), \
                    patch.object(self.module, "qb_login", return_value=False) as mock_login, \
                    patch.object(self.module, "get_qb_torrents") as mock_torrents:
                self.module.get_qb_downloads(temp_dir)

            mock_login.assert_called_once_with(fake_session)
            mock_torrents.assert_not_called()
            self.assertEqual(list(Path(temp_dir).rglob("*")), [])

    def test_get_qb_downloads_returns_early_when_no_torrents(self):
        fake_session = Mock(name="session")

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(self.module.requests, "Session", return_value=fake_session), \
                    patch.object(self.module, "qb_login", return_value=True), \
                    patch.object(self.module, "get_qb_torrents", return_value=[]) as mock_torrents:
                self.module.get_qb_downloads(temp_dir)

            mock_torrents.assert_called_once_with(fake_session)
            self.assertEqual(list(Path(temp_dir).rglob("*")), [])


class TestGetQbTorrents(unittest.TestCase):
    def setUp(self):
        self.module = load_get_qb_downloads()

    def test_get_qb_torrents_requests_downloading_info(self):
        response = Mock()
        response.status_code = 200
        response.json.return_value = [{"name": "movie"}]

        session = Mock()
        session.get.return_value = response

        torrents = self.module.get_qb_torrents(session)

        session.get.assert_called_once_with("https://example.com/qb/api/v2/torrents/info?filter=downloading")
        self.assertEqual(torrents, [{"name": "movie"}])

    def test_get_qb_torrents_logs_and_returns_none_on_http_error(self):
        response = Mock()
        response.status_code = 403
        response.text = "Forbidden"

        session = Mock()
        session.get.return_value = response

        with patch.object(self.module.logger, "error") as mock_error:
            torrents = self.module.get_qb_torrents(session)

        self.assertIsNone(torrents)
        mock_error.assert_called_once_with("请求 QB 失败：403 Forbidden")


if __name__ == "__main__":
    unittest.main()
