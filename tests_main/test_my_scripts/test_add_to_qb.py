"""
针对 ``my_scripts.add_to_qb`` 的单元测试。

这些测试不依赖真实的 qBittorrent 服务、真实配置文件或真实网络。
重点验证 review 中指出的几个行为缺口：
1. 只统计真正成功的提交。
2. 非 Ok. 的 API 返回应触发失败语义，而不是静默继续。
3. 坏 JSON / 空 LOG 不应中断整批任务。
4. 请求应包含 timeout，并处理 requests 异常。
5. 上传 torrent 文件时应可靠关闭文件句柄。
6. JSON 磁链应使用本模块配置的 magnet_path，而不是隐藏依赖别处配置。
"""

import importlib.util
import io
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import ANY, Mock, call, patch

import requests

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "add_to_qb.py"


def load_add_to_qb(config: dict | None = None, select_yts_best_torrent=None):
    """在隔离环境中加载 ``add_to_qb`` 模块。"""
    module_config = {
        "qb_url": "https://example.com/qb",
        "qb_user": "user",
        "qb_pass": "pass",
        "qb_save_dir": "/downloads/qb",
        "magnet_path": "magnet:?xt=urn:btih:",
    }
    if config:
        module_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: dict(module_config)

    fake_sort_movie_ops = types.ModuleType("sort_movie_ops")
    if select_yts_best_torrent is None:
        fake_sort_movie_ops.extract_torrent_download_link = lambda _path, _magnet_path: "magnet:?xt=urn:btih:FAKEHASH"
    else:
        fake_sort_movie_ops.extract_torrent_download_link = lambda _path, _magnet_path: select_yts_best_torrent(build_movie_json())

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    spec = importlib.util.spec_from_file_location(
        f"add_to_qb_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "sort_movie_ops": fake_sort_movie_ops,
            "retrying": fake_retrying,
        },
    ):
        spec.loader.exec_module(module)

    return module


def build_movie_json(hash_value: str = "JSONHASH") -> dict:
    """构造一个最小可用的 YTS JSON 结构。"""
    return {
        "data": {
            "movie": {
                "torrents": [
                    {
                        "quality": "1080p",
                        "video_codec": "x265",
                        "bit_depth": "10",
                        "type": "bluray",
                        "size_bytes": 1000,
                        "hash": hash_value,
                    }
                ]
            }
        }
    }


class TestAddToQbMain(unittest.TestCase):
    def test_add_to_qb_counts_supported_files(self):
        module = load_add_to_qb(
            select_yts_best_torrent=lambda json_data: f"magnet:?xt=urn:btih:{json_data['data']['movie']['torrents'][0]['hash']}"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "Director"
            root.mkdir()
            (root / "first.log").write_text("placeholder", encoding="utf-8")
            (root / "second.log").write_text("placeholder", encoding="utf-8")

            fake_session = Mock(name="session")

            with patch.object(module.requests, "Session", return_value=fake_session), \
                    patch.object(module, "qb_login", return_value=True), \
                    patch.object(module, "extract_torrent_download_link", side_effect=["magnet:?xt=urn:btih:FIRST", "magnet:?xt=urn:btih:SECOND"]), \
                    patch.object(module, "add_magnet_link", side_effect=[False, True]), \
                    patch.object(module.logger, "info") as mock_logger_info:
                module.add_to_qb(temp_dir)

        self.assertIn(call("共添加 2 个任务。"), mock_logger_info.call_args_list)

    def test_add_to_qb_skips_bad_json_and_empty_log_instead_of_crashing(self):
        module = load_add_to_qb()

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "Director"
            root.mkdir()
            (root / "bad.json").write_text("{}", encoding="utf-8")
            (root / "empty.log").write_text("", encoding="utf-8")

            fake_session = Mock(name="session")

            with patch.object(module.requests, "Session", return_value=fake_session), \
                    patch.object(module, "qb_login", return_value=True), \
                    patch.object(module, "extract_torrent_download_link", return_value=None), \
                    patch.object(module, "add_magnet_link") as mock_add_magnet, \
                    patch.object(module.logger, "info") as mock_logger_info, \
                    patch.object(module.logger, "error"):
                module.add_to_qb(temp_dir)

        mock_add_magnet.assert_not_called()
        self.assertIn(call("共添加 0 个任务。"), mock_logger_info.call_args_list)

    def test_add_to_qb_uses_configured_magnet_path_for_json_downloads(self):
        module = load_add_to_qb(
            config={"magnet_path": "config-prefix:"},
            select_yts_best_torrent=lambda _json_data: "wrong-prefix:HASH",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "Director"
            root.mkdir()
            (root / "movie.json").write_text("{}", encoding="utf-8")

            fake_session = Mock(name="session")

            def fake_extract(path, magnet_path):
                self.assertEqual(magnet_path, module.MAGNET_PATH)
                self.assertEqual(Path(path).name, "movie.json")
                return "config-prefix:JSONHASH"

            with patch.object(module.requests, "Session", return_value=fake_session), \
                    patch.object(module, "qb_login", return_value=True), \
                    patch.object(module, "extract_torrent_download_link", side_effect=fake_extract), \
                    patch.object(module, "add_magnet_link") as mock_add_magnet:
                module.add_to_qb(temp_dir)

        mock_add_magnet.assert_called_once_with(
            fake_session,
            "config-prefix:JSONHASH",
            save_path="/downloads/qb/Director/movie",
            tags="Director",
            category="ytf",
        )


class TestQbApiHelpers(unittest.TestCase):
    def test_qb_login_returns_false_on_request_exception(self):
        module = load_add_to_qb()
        session = Mock()
        session.post.side_effect = requests.RequestException("boom")

        with patch.object(module.logger, "error"):
            result = module.qb_login(session)

        self.assertFalse(result)

    def test_qb_login_passes_timeout(self):
        module = load_add_to_qb()
        session = Mock()
        session.post.return_value = Mock(text="Ok.")

        module.qb_login(session)

        self.assertIn("timeout", session.post.call_args.kwargs)

    def test_add_magnet_link_raises_on_rejected_response(self):
        module = load_add_to_qb()
        session = Mock()
        session.post.return_value = Mock(status_code=200, text="Fails.")

        with patch.object(module.logger, "error"), self.assertRaises(RuntimeError):
            module.add_magnet_link(
                session,
                "magnet:?xt=urn:btih:HASH",
                save_path="/downloads/qb/Director/movie",
                tags="Director",
                category="ru",
            )

    def test_add_magnet_link_passes_timeout(self):
        module = load_add_to_qb()
        session = Mock()
        session.post.return_value = Mock(status_code=200, text="Ok.")

        module.add_magnet_link(
            session,
            "magnet:?xt=urn:btih:HASH",
            save_path="/downloads/qb/Director/movie",
            tags="Director",
            category="ru",
        )

        self.assertIn("timeout", session.post.call_args.kwargs)

    def test_add_torrent_file_closes_handle_when_post_raises(self):
        module = load_add_to_qb()
        session = Mock()
        session.post.side_effect = requests.RequestException("boom")
        file_obj = io.BytesIO(b"torrent-bytes")

        with patch("builtins.open", return_value=file_obj):
            with self.assertRaises(requests.RequestException):
                module.add_torrent_file(
                    session,
                    r"C:\temp\movie.torrent",
                    save_path="/downloads/qb/Director/movie",
                    tags="Director",
                    category="ru",
                )

        self.assertTrue(file_obj.closed)

    def test_add_torrent_file_passes_timeout(self):
        module = load_add_to_qb()
        session = Mock()
        session.post.return_value = Mock(status_code=200, text="Ok.")
        file_obj = io.BytesIO(b"torrent-bytes")

        with patch("builtins.open", return_value=file_obj):
            module.add_torrent_file(
                session,
                r"C:\temp\movie.torrent",
                save_path="/downloads/qb/Director/movie",
                tags="Director",
                category="ru",
            )

        self.assertIn("timeout", session.post.call_args.kwargs)


if __name__ == "__main__":
    unittest.main()
