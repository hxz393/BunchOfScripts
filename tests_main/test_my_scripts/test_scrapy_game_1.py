"""
针对 ``my_scripts.scrapy_game_1`` 的定向单元测试。

这些测试不依赖真实配置文件或真实网络，只覆盖当前准备修整的核心行为：
1. 主流程在单页失败时仍继续处理其他链接，并统计失败数。
2. 页面抓取、HTML 解析和百度跳转提取的关键分支。
3. 结果文件的写出格式。
"""

import copy
import importlib.util
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, call, patch

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_game_1.py"


def load_scrapy_game_1(config: dict | None = None):
    """在隔离依赖的环境中加载 ``scrapy_game_1`` 模块。"""
    module_config = {
        "scrapy_game_1": {
            "base_url": "https://example.com",
            "start_number": 0,
            "stop_number": 2,
            "output_txt": "output.txt",
            "user_cookie": "cookie=value",
            "thread_number": 2,
            "proxies_list": [{"https": "http://127.0.0.1:7890"}],
            "request_head": {"User-Agent": "unit-test"},
        }
    }
    if config:
        module_config["scrapy_game_1"].update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: copy.deepcopy(module_config)

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    spec = importlib.util.spec_from_file_location(
        f"scrapy_game_1_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "retrying": fake_retrying,
        },
    ):
        spec.loader.exec_module(module)

    return module


class ImmediateFuture:
    """同步 future，方便控制 ``as_completed`` 顺序。"""

    def __init__(self, value=None, exc: Exception | None = None):
        self._value = value
        self._exc = exc

    def result(self):
        if self._exc is not None:
            raise self._exc
        return self._value


class ImmediateExecutor:
    """同步执行器，实现被测代码所需的最小接口。"""

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def submit(self, func, *args, **kwargs):
        try:
            return ImmediateFuture(func(*args, **kwargs))
        except Exception as exc:
            return ImmediateFuture(exc=exc)


class BrokenExecutor:
    """进入线程池上下文时直接抛错，用于验证外层异常处理。"""

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        raise RuntimeError("executor boom")

    def __exit__(self, exc_type, exc, tb):
        return False


class TestScrapyGameMain(unittest.TestCase):
    def setUp(self):
        self.module = load_scrapy_game_1()

    def test_scrapy_game_1_continues_after_partial_failure(self):
        results_by_link = {
            "https://example.com/game/1.html": ("https://example.com/game/1.html", "", ""),
            "https://example.com/game/2.html": ("https://example.com/game/2.html", "Title 2", "https://pan.baidu.com/s/2 code2"),
        }

        with patch.object(self.module, "main", side_effect=lambda link: results_by_link[link]), \
                patch.object(self.module, "handle_result") as mock_handle, \
                patch.object(self.module, "ThreadPoolExecutor", ImmediateExecutor), \
                patch.object(self.module.concurrent.futures, "as_completed", side_effect=lambda futures: list(futures)), \
                patch.object(self.module.logger, "info") as mock_info:
            self.module.scrapy_game_1()

        mock_handle.assert_called_once_with(
            ("https://example.com/game/2.html", "Title 2", "https://pan.baidu.com/s/2 code2"),
            "https://example.com/game/2.html",
        )
        self.assertIn(call("总计数量：2，失败数量：1"), mock_info.call_args_list)

    def test_main_returns_empty_result_when_page_fetch_fails(self):
        with patch.object(self.module, "fetch_web_page", return_value=""):
            result = self.module.main("https://example.com/game/9.html")

        self.assertEqual(result, ("https://example.com/game/9.html", "", ""))

    def test_main_returns_title_without_baidu_link_when_redirect_missing(self):
        parse_result = {
            "link": "https://example.com/game/3.html",
            "title": "Game 3",
            "password": "abcd",
            "fetched_link": "/go/3",
        }

        with patch.object(self.module, "fetch_web_page", return_value="<html></html>"), \
                patch.object(self.module, "parse_web_content", return_value=parse_result), \
                patch.object(self.module, "fetch_baidu_link", return_value=""):
            result = self.module.main("https://example.com/game/3.html")

        self.assertEqual(result, ("https://example.com/game/3.html", "Game 3", ""))

    def test_scrapy_game_1_logs_executor_setup_error_without_unbound_link(self):
        with patch.object(self.module, "ThreadPoolExecutor", BrokenExecutor), \
                patch.object(self.module.logger, "exception") as mock_exception, \
                patch.object(self.module.logger, "info"):
            self.module.scrapy_game_1()

        self.assertIn(call("分配线程时发生错误"), mock_exception.call_args_list)


class TestScrapyGameHelpers(unittest.TestCase):
    def setUp(self):
        self.module = load_scrapy_game_1()

    def test_fetch_web_page_uses_proxy_and_returns_html(self):
        response = Mock()
        response.text = "<html>ok</html>"
        response.raise_for_status.return_value = None

        with patch.object(self.module.random, "choice", return_value={"https": "http://proxy"}), \
                patch.object(self.module.requests, "get", return_value=response) as mock_get:
            html = self.module.fetch_web_page("https://example.com/game/5.html")

        self.assertEqual(html, "<html>ok</html>")
        mock_get.assert_called_once_with(
            "https://example.com/game/5.html",
            headers=self.module.REQUEST_HEAD,
            timeout=15,
            verify=False,
            allow_redirects=False,
            proxies={"https": "http://proxy"},
        )

    def test_parse_web_content_extracts_title_password_and_fetch_link(self):
        html = """
        <html>
          <div class="article-tit"><h1>  Sample Game  </h1></div>
          <a href="javascript:;" class="downbtn normal" data-info='pw12' data-url="/go/abc"><i></i>百度网盘</a>
        </html>
        """

        result = self.module.parse_web_content("https://example.com/game/1.html", html)

        self.assertEqual(
            result,
            {
                "link": "https://example.com/game/1.html",
                "title": "Sample Game",
                "password": "pw12",
                "fetched_link": "/go/abc",
            },
        )

    def test_fetch_baidu_link_returns_redirect_and_password(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.headers = {"location": "https://pan.baidu.com/s/abc"}

        fetch_web_response = {
            "link": "https://example.com/game/1.html",
            "title": "Game 1",
            "password": "pw12",
            "fetched_link": "/go/abc",
        }

        with patch.object(self.module.random, "choice", return_value={"https": "http://proxy"}), \
                patch.object(self.module.requests, "get", return_value=response) as mock_get:
            result = self.module.fetch_baidu_link(fetch_web_response)

        self.assertEqual(result, "https://pan.baidu.com/s/abc pw12")
        mock_get.assert_called_once_with(
            "https://example.com/go/abc",
            headers=self.module.REQUEST_HEAD,
            timeout=15,
            verify=False,
            allow_redirects=False,
            proxies={"https": "http://proxy"},
        )

    def test_write_results_appends_expected_format(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = Path(temp_dir) / "results.txt"
            ok = self.module.write_results(
                [("https://example.com/game/1.html", "Title 1", "https://pan.baidu.com/s/1 code1")],
                output_file=str(output_file),
            )

            self.assertTrue(ok)
            self.assertEqual(
                output_file.read_text(encoding="utf-8"),
                "https://example.com/game/1.html\n"
                "Title 1\n"
                "https://pan.baidu.com/s/1 code1\n"
                "****************************************************\n",
            )


if __name__ == "__main__":
    unittest.main()
