"""
针对 ``my_scripts.scrapy_rls`` 的单元测试。

这里不依赖真实配置文件，也不会发出真实网络请求。
目标是只验证抓取脚本的核心行为：
1. 列表页请求、状态分支和 HTML 解析。
2. 详情页 IMDb 提取、文件名组装和写盘逻辑。
3. 并发处理和主入口翻页/重试停止条件。
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

import requests

requests.packages.urllib3.disable_warnings()

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_rls.py"


def load_scrapy_rls(config: dict | None = None):
    """
    在隔离环境中加载 ``scrapy_rls`` 模块。

    被测模块在 import 时就会读取配置并导入 ``my_module`` / ``retrying``，
    这里先注入假的依赖，避免测试依赖本地真实配置和真实网络环境。
    """
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "rls_url": "https://example.com/",
        "rls_cookie": "cookie=value",
        "request_head": {"User-Agent": "unit-test"},
        "output_dir": temp_dir.name,
    }
    if config:
        module_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: copy.deepcopy(module_config)
    fake_my_module.normalize_release_title_for_filename = lambda title: title
    fake_my_module.sanitize_filename = lambda name: name

    def fake_write_list_to_file(path: str, content: list[str]) -> bool:
        target_path = Path(path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text("\n".join(content), encoding="utf-8")
        return True

    fake_my_module.write_list_to_file = fake_write_list_to_file

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    spec = importlib.util.spec_from_file_location(
        f"scrapy_rls_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"my_module": fake_my_module, "retrying": fake_retrying}):
        spec.loader.exec_module(module)

    return module, temp_dir


def build_rls_item_html(title: str = "Movie Title 2026", href: str = "https://example.com/post/1") -> str:
    """构造一条最小可用的 RLS 列表项 HTML。"""
    return f'<div class="p-c p-c-title"><h2><a href="{href}">{title}</a></h2></div>'


def build_rls_page_html(*items: str) -> str:
    """把若干 RLS 列表项拼成最小可用页面。"""
    return f"<html><body>{''.join(items)}</body></html>"


def build_detail_page_html(*hrefs: str) -> str:
    """构造最小可用详情页 HTML。"""
    links = "".join(f'<a href="{href}">link</a>' for href in hrefs)
    return f"<html><body>{links}</body></html>"


class TestModuleLoad(unittest.TestCase):
    """验证模块导入时的配置注入。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rls()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_scrapy_rls_injects_cookie_into_request_head(self):
        """模块加载时应把配置里的 cookie 注入请求头。"""
        self.assertEqual(self.module.REQUEST_HEAD["Cookie"], "cookie=value")


class TestGetRlsResponse(unittest.TestCase):
    """验证列表页请求逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rls()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_rls_response_returns_response_and_sets_utf8_encoding(self):
        """请求成功时应返回响应对象，并统一设置 UTF-8 编码。"""
        response = Mock(status_code=200)

        with patch.object(self.module.requests, "get", return_value=response) as mock_get:
            result = self.module.get_rls_response("https://example.com/page/1")

        self.assertIs(result, response)
        self.assertEqual(response.encoding, "utf-8")
        mock_get.assert_called_once_with(
            "https://example.com/page/1",
            timeout=35,
            headers=self.module.REQUEST_HEAD,
        )

    def test_get_rls_response_exits_when_status_code_is_403(self):
        """遇到 403 时应通过 ``sys.exit`` 直接终止。"""
        response = Mock(status_code=403)

        with patch.object(self.module.requests, "get", return_value=response):
            with self.assertRaisesRegex(SystemExit, "403"):
                self.module.get_rls_response("https://example.com/page/1")

    def test_get_rls_response_raises_when_status_code_is_not_200(self):
        """请求返回非 200 状态码时应抛出异常。"""
        response = Mock(status_code=503)

        with patch.object(self.module.requests, "get", return_value=response):
            with self.assertRaisesRegex(Exception, "503"):
                self.module.get_rls_response("https://example.com/page/1")


class TestParseRlsResponse(unittest.TestCase):
    """验证列表页解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rls()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_rls_response_returns_titles_and_urls(self):
        """应提取标题、链接，并清理星标和空格。"""
        response = Mock(
            text=build_rls_page_html(
                build_rls_item_html("Movie Title ⭐ 2026", "https://example.com/post/1"),
                build_rls_item_html("Another Title 2025", "https://example.com/post/2"),
            )
        )

        result = self.module.parse_rls_response(response)

        self.assertEqual(
            result,
            [
                {"title": "Movie.Title.2026", "url": "https://example.com/post/1"},
                {"title": "Another.Title.2025", "url": "https://example.com/post/2"},
            ],
        )

    def test_parse_rls_response_returns_empty_list_when_no_posts_exist(self):
        """页面中没有帖子时应返回空列表。"""
        response = Mock(text="<html><body><p>empty</p></body></html>")

        self.assertEqual(self.module.parse_rls_response(response), [])


class TestVisitRlsUrl(unittest.TestCase):
    """验证详情页 IMDb 提取和落盘逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rls()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_visit_rls_url_extracts_canonical_imdb_link_and_writes_file(self):
        """存在标准 IMDb 链接时，应提取编号并写出 ``.rls`` 文件。"""
        result_item = {
            "title": "Movie.Title.2026",
            "url": "https://example.com/post/1",
        }
        response = Mock(
            text=build_detail_page_html(
                "https://example.com/other",
                "https://www.imdb.com/title/tt7654321/",
            )
        )

        with patch.object(self.module, "get_rls_response", return_value=response):
            self.module.visit_rls_url(result_item)

        output_path = Path(self.temp_dir.name) / "Movie.Title.2026 - rls [tt7654321].rls"
        self.assertEqual(result_item["imdb"], "tt7654321")
        self.assertTrue(output_path.exists())
        self.assertEqual(output_path.read_text(encoding="utf-8"), "https://example.com/post/1")

    def test_visit_rls_url_uses_loose_tt_match_when_canonical_imdb_link_is_missing(self):
        """没有标准 IMDb 链接时，应回退到宽松 ``tt`` 提取。"""
        result_item = {
            "title": "Loose.Match.2026",
            "url": "https://example.com/post/2",
        }
        response = Mock(
            text=build_detail_page_html(
                "https://example.com/jump?target=tt1234567",
            )
        )

        with patch.object(self.module, "get_rls_response", return_value=response):
            self.module.visit_rls_url(result_item)

        output_path = Path(self.temp_dir.name) / "Loose.Match.2026 - rls [tt1234567].rls"
        self.assertEqual(result_item["imdb"], "tt1234567")
        self.assertTrue(output_path.exists())

    def test_visit_rls_url_writes_empty_imdb_when_no_match_exists(self):
        """页面不存在任何 IMDb 编号时，应写出空编号文件。"""
        result_item = {
            "title": "No.Imdb.2026",
            "url": "https://example.com/post/3",
        }
        response = Mock(text=build_detail_page_html("https://example.com/without-imdb"))

        with patch.object(self.module, "get_rls_response", return_value=response):
            self.module.visit_rls_url(result_item)

        output_path = Path(self.temp_dir.name) / "No.Imdb.2026 - rls [].rls"
        self.assertEqual(result_item["imdb"], "")
        self.assertTrue(output_path.exists())


class TestProcessAll(unittest.TestCase):
    """验证并发处理逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rls()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_process_all_collects_success_results_and_logs_failures(self):
        """成功任务应进入返回值，失败任务应记录错误日志。"""
        items = [
            {"url": "ok-1"},
            {"url": "boom"},
            {"url": "ok-2"},
        ]

        def fake_visit(item: dict):
            if item["url"] == "boom":
                raise RuntimeError("bad item")
            return item["url"]

        with patch.object(self.module, "visit_rls_url", side_effect=fake_visit), patch.object(
            self.module.logger, "error"
        ) as mock_error:
            result = self.module.process_all(items, max_workers=2)

        self.assertCountEqual(result, ["ok-1", "ok-2"])
        mock_error.assert_called_once()
        self.assertIn("boom", mock_error.call_args[0][0])


class TestScrapyRlsMain(unittest.TestCase):
    """验证主抓取流程的编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rls()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_rls_retries_empty_page_until_results_are_parsed(self):
        """当前页解析为空时，应等待后重试同一页。"""
        end_title = "Stop Here 2026"
        parsed_page = [{"title": "Stop.Here.2026", "url": "https://example.com/post/1"}]

        with patch.object(self.module, "get_rls_response", return_value=Mock(text="html")) as mock_get, patch.object(
            self.module, "parse_rls_response", side_effect=[[], parsed_page]
        ) as mock_parse, patch.object(
            self.module, "process_all"
        ) as mock_process, patch.object(
            self.module.time, "sleep"
        ) as mock_sleep:
            self.module.scrapy_rls(start_page=3, f_mode=False, end_title=end_title)

        expected_url = "https://example.com/category/movies/page/3/?s="
        self.assertEqual(mock_get.call_args_list, [call(expected_url), call(expected_url)])
        self.assertEqual(mock_parse.call_count, 2)
        mock_sleep.assert_called_once_with(3)
        mock_process.assert_called_once_with(parsed_page, max_workers=40)

    def test_scrapy_rls_advances_pages_until_end_title_is_found(self):
        """命中截止标题前应继续翻页，命中后停止。"""
        page_one = [{"title": "Fresh.Release.2026", "url": "https://example.com/post/1"}]
        page_two = [{"title": "Stop.Here.2026", "url": "https://example.com/post/2"}]

        with patch.object(self.module, "get_rls_response", return_value=Mock(text="html")) as mock_get, patch.object(
            self.module, "parse_rls_response", side_effect=[page_one, page_two]
        ), patch.object(
            self.module, "process_all"
        ) as mock_process:
            self.module.scrapy_rls(start_page=1, end_title="Stop Here 2026")

        self.assertEqual(
            mock_get.call_args_list,
            [
                call("https://example.com/category/foreign-movies/page/1/?s="),
                call("https://example.com/category/foreign-movies/page/2/?s="),
            ],
        )
        self.assertEqual(mock_process.call_args_list, [call(page_one, max_workers=40), call(page_two, max_workers=40)])


if __name__ == "__main__":
    unittest.main()
