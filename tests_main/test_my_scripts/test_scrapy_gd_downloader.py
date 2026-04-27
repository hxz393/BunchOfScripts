"""
针对 ``my_scripts.scrapy_gd_downloader`` 的单元测试。
"""

import importlib.util
import sys
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

import requests

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_gd_downloader.py"


class FakeSession:
    """最小 requests.Session 实现。"""

    def __init__(self):
        self.proxies = {}
        self.mount_calls = []

    def mount(self, prefix: str, adapter):
        self.mount_calls.append((prefix, adapter))

    def get(self, *args, **kwargs):  # pragma: no cover - 测试中按需打桩
        raise AssertionError("session.get should be patched in tests")


def fake_retry(*args, **kwargs):
    """最小 retry 装饰器实现，支持按最大次数重试。"""
    max_attempts = kwargs.get("stop_max_attempt_number", 1)

    def decorator(func):
        def wrapper(*func_args, **func_kwargs):
            last_exception = None
            for _ in range(max_attempts):
                try:
                    return func(*func_args, **func_kwargs)
                except Exception as exc:  # pragma: no cover
                    last_exception = exc
            raise last_exception

        return wrapper

    return decorator


def load_scrapy_gd_downloader():
    """在隔离环境中加载 ``scrapy_gd_downloader`` 模块。"""
    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = fake_retry
    spec = importlib.util.spec_from_file_location(
        f"scrapy_gd_downloader_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"retrying": fake_retrying}), patch.object(requests, "Session", FakeSession):
        spec.loader.exec_module(module)

    return module


class TestModuleLoad(unittest.TestCase):
    """验证模块导入时的会话初始化。"""

    def setUp(self):
        self.module = load_scrapy_gd_downloader()

    def test_load_scrapy_gd_downloader_initializes_session(self):
        """模块加载时应创建无代理、但带更大连接池的 Session。"""
        self.assertEqual(
            self.module.session.proxies,
            {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"},
        )
        self.assertEqual([prefix for prefix, _adapter in self.module.session.mount_calls], ["http://", "https://"])

    def test_build_session_expands_connection_pool(self):
        with patch.object(self.module.requests, "Session", FakeSession):
            session = self.module.build_session(37)

        self.assertEqual(
            session.proxies,
            {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"},
        )
        self.assertEqual([prefix for prefix, _adapter in session.mount_calls], ["http://", "https://"])
        self.assertEqual(session.mount_calls[0][1]._pool_connections, 37)
        self.assertEqual(session.mount_calls[0][1]._pool_maxsize, 37)


class TestExtractDriveUrls(unittest.TestCase):
    """验证 Drive 链接提取逻辑。"""

    def setUp(self):
        self.module = load_scrapy_gd_downloader()

    def test_extract_drive_urls_deduplicates_and_preserves_order(self):
        """应去重并保留原始出现顺序。"""
        html_text = (
            '<a href="https://drive.google.com/file/d/file111/view?usp=sharing">one</a>'
            '<a href="https://www.imdb.com/title/tt1234567/">IMDb</a>'
            '<a href="https://drive.google.com/file/d/file222/view?usp=sharing">two</a>'
            '<a href="https://drive.google.com/file/d/file111/view?usp=sharing">dup</a>'
        )

        result = self.module.extract_drive_urls(html_text)

        self.assertEqual(
            result,
            [
                "https://drive.google.com/file/d/file111/view?usp=sharing",
                "https://drive.google.com/file/d/file222/view?usp=sharing",
            ],
        )


class TestDownloadGdUrl(unittest.TestCase):
    """验证 Google Drive 下载逻辑。"""

    def setUp(self):
        self.module = load_scrapy_gd_downloader()

    def test_download_gd_url_returns_payload_and_suffix(self):
        """应返回下载内容、原始文件名和建议后缀。"""
        view_response = Mock(
            status_code=200,
            text=(
                '<meta itemprop="name" content="Movie.nzb">'
                'https://drive.usercontent.google.com/uc?id=file123&export=download'
            ),
            content=b"",
            headers={"Content-Type": "text/html; charset=utf-8"},
        )
        download_response = Mock(
            status_code=200,
            text="",
            content=b'<?xml version="1.0"?><nzb></nzb>',
            headers={"Content-Type": "application/octet-stream"},
        )

        with patch.object(self.module.session, "get", side_effect=[view_response, download_response]):
            result = self.module.download_gd_url("https://drive.google.com/file/d/file123/view?usp=sharing")

        self.assertEqual(result.drive_name, "Movie.nzb")
        self.assertEqual(result.payload, b'<?xml version="1.0"?><nzb></nzb>')
        self.assertEqual(result.suggested_suffix, ".nzb")


if __name__ == "__main__":
    unittest.main()
