"""
针对 ``my_scripts.scrapy_hde`` 的单元测试。

这里在隔离环境中加载模块，不依赖真实配置，也不会发出真实网络请求。
主要验证请求、列表页解析、大小提取、详情页落盘和分页停止条件。
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

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_hde.py"


def load_scrapy_hde(config: dict | None = None):
    """在最小依赖环境中加载 ``scrapy_hde`` 模块。"""
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "hde_url": "https://example.com/",
        "output_dir": str(Path(temp_dir.name) / "downloads"),
        "default_end_title": "Old Movie – 1.0 GB",
        "max_workers": 30,
        "default_release_size": "100.0 GB",
        "request_timeout_seconds": 30,
        "retry_max_attempts": 150,
        "retry_wait_min_ms": 1000,
        "retry_wait_max_ms": 10000,
    }
    if config:
        module_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: copy.deepcopy(module_config)
    fake_my_module.normalize_release_title_for_filename = lambda title: title.replace("/", "｜")
    fake_my_module.sanitize_filename = lambda name: name.replace(":", "_")

    def fake_write_list_to_file(path: str, content: list[str]) -> bool:
        target_path = Path(path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text("\n".join(content), encoding="utf-8")
        return True

    fake_my_module.write_list_to_file = fake_write_list_to_file

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    spec = importlib.util.spec_from_file_location(
        f"scrapy_hde_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"my_module": fake_my_module, "retrying": fake_retrying}):
        spec.loader.exec_module(module)

    return module, temp_dir


def build_fit_item(title: str, href: str) -> str:
    """构造一个最小可用的列表页条目。"""
    return f"""
    <div class="fit item">
      <div class="data">
        <h5><a href="{href}">{title}</a></h5>
      </div>
    </div>
    """


def build_list_page(*items: str) -> str:
    """构造一个最小可用的列表页 HTML。"""
    return f"<html><body>{''.join(items)}</body></html>"


def build_detail_page(*hrefs: str) -> str:
    """构造一个最小可用的详情页 HTML。"""
    return "<html><body>" + "".join(f'<a href="{href}">id</a>' for href in hrefs) + "</body></html>"


class TestModuleLoad(unittest.TestCase):
    """验证模块导入时的配置注入。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_scrapy_hde_uses_injected_config(self):
        """模块加载后应暴露注入的站点地址、输出目录和配置化默认值。"""
        self.assertEqual(self.module.HDE_URL, "https://example.com/")
        self.assertTrue(str(self.module.OUTPUT_DIR).endswith("downloads"))
        self.assertEqual(self.module.DEFAULT_END_TITLE, "Old Movie – 1.0 GB")
        self.assertEqual(self.module.DEFAULT_MAX_WORKERS, 30)


class TestHdeHelpers(unittest.TestCase):
    """验证主流程辅助函数。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_build_hde_page_url_formats_archive_page_address(self):
        """分页 URL 应基于配置的站点前缀拼接。"""
        self.assertEqual(
            self.module.build_hde_page_url(3),
            "https://example.com/tag/movies/page/3/",
        )

    def test_should_stop_scrapy_returns_true_only_when_end_title_is_present(self):
        """截止标题存在时应停止翻页，否则继续。"""
        result_list = [
            {"title": "New Movie – 2.0 GB", "url": "https://example.com/new", "size": "2.0GB"},
            {"title": "Old Movie – 1.0 GB", "url": "https://example.com/old", "size": "1.0GB"},
        ]

        self.assertTrue(self.module.should_stop_scrapy(result_list, "Old Movie – 1.0 GB"))
        self.assertFalse(self.module.should_stop_scrapy(result_list, "Missing Movie"))


class TestGetHdeResponse(unittest.TestCase):
    """验证单次请求逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_hde_response_returns_response_and_sets_utf8_encoding(self):
        """请求成功时应返回响应对象，并统一设置 UTF-8 编码。"""
        response = Mock(status_code=200)

        with patch.object(self.module.requests, "get", return_value=response) as mock_get:
            result = self.module.get_hde_response("https://example.com/post")

        self.assertIs(result, response)
        self.assertEqual(response.encoding, "utf-8")
        mock_get.assert_called_once_with("https://example.com/post", timeout=30)

    def test_get_hde_response_raises_when_status_code_is_not_200(self):
        """请求返回非 200 状态码时应抛出异常。"""
        response = Mock(status_code=503)

        with patch.object(self.module.requests, "get", return_value=response):
            with self.assertRaisesRegex(Exception, "503"):
                self.module.get_hde_response("https://example.com/post")

    def test_get_hde_response_propagates_request_exception(self):
        """底层请求异常时应直接抛出，交给重试装饰器处理。"""
        with patch.object(self.module.requests, "get", side_effect=requests.Timeout("timed out")):
            with self.assertRaisesRegex(requests.Timeout, "timed out"):
                self.module.get_hde_response("https://example.com/post")


class TestParseHdeResponse(unittest.TestCase):
    """验证列表页解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_hde_response_extracts_title_and_url(self):
        """应从列表页提取标题、链接和体积。"""
        response = Mock(
            text=build_list_page(
                build_fit_item(
                    title="Movie Title – 1.2 GB",
                    href="https://example.com/post-1",
                )
            )
        )

        result = self.module.parse_hde_response(response)

        self.assertEqual(
            result,
            [
                {
                    "title": "Movie Title – 1.2 GB",
                    "url": "https://example.com/post-1",
                    "size": "1.2GB",
                }
            ],
        )

    def test_parse_hde_response_skips_entries_without_data_or_anchor(self):
        """条目缺少 ``div.data`` 或标题链接时应跳过。"""
        response = Mock(
            text=build_list_page(
                '<div class="fit item"><div class="other"></div></div>',
                '<div class="fit item"><div class="data"><h5>No link</h5></div></div>',
                build_fit_item(
                    title="Valid Movie",
                    href="https://example.com/post-2",
                ),
            )
        )

        result = self.module.parse_hde_response(response)

        self.assertEqual(
            result,
            [
                {
                    "title": "Valid Movie",
                    "url": "https://example.com/post-2",
                    "size": "100.0GB",
                }
            ],
        )

    def test_parse_hde_item_returns_none_when_required_nodes_are_missing(self):
        """单条条目缺少 ``div.data`` 或链接时应返回 ``None``。"""
        fit_without_data = self.module.BeautifulSoup(
            '<div class="fit item"><div class="other"></div></div>',
            "html.parser",
        ).select_one("div.fit.item")
        fit_without_link = self.module.BeautifulSoup(
            '<div class="fit item"><div class="data"><h5>No link</h5></div></div>',
            "html.parser",
        ).select_one("div.fit.item")

        self.assertIsNone(self.module.parse_hde_item(fit_without_data))
        self.assertIsNone(self.module.parse_hde_item(fit_without_link))


class TestReleaseSize(unittest.TestCase):
    """验证大小提取逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_extract_release_size_extracts_size_from_dash_suffix_or_trailing_size(self):
        """应优先提取破折号后的大小，其次回退到末尾大小。"""
        self.assertEqual(self.module.extract_release_size("Movie One – 22.4 GB"), "22.4GB")
        self.assertEqual(self.module.extract_release_size("Movie Two 700 MB"), "700MB")

    def test_extract_release_size_returns_normalized_default_when_size_is_missing(self):
        """没有体积信息时应返回去空格后的默认值。"""
        self.assertEqual(self.module.extract_release_size("Movie Three"), "100.0GB")
        self.assertEqual(self.module.extract_release_size("Movie Three", default_size="1.5 TB"), "1.5TB")


class TestProcessAll(unittest.TestCase):
    """验证批量多线程编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_process_all_collects_successful_results(self):
        """批处理应执行全部任务，不再返回无意义的结果列表。"""
        items = [{"title": "A"}, {"title": "B"}]

        with patch.object(self.module, "visit_hde_url", side_effect=lambda item: f"done:{item['title']}"):
            result = self.module.process_all(items, max_workers=1)

        self.assertIsNone(result)

    def test_process_all_logs_errors_without_raising(self):
        """单个任务失败时，应记录错误且不影响其他任务。"""
        items = [{"title": "good"}, {"title": "bad"}]

        def fake_visit(item: dict):
            if item["title"] == "bad":
                raise RuntimeError("boom")
            return f"done:{item['title']}"

        with patch.object(self.module, "visit_hde_url", side_effect=fake_visit), self.assertLogs(
            self.module.logger.name,
            level="ERROR",
        ) as logs:
            result = self.module.process_all(items, max_workers=1)

        self.assertIsNone(result)
        self.assertIn("[ERROR] {'title': 'bad'} -> RuntimeError('boom')", logs.output[0])


class TestVisitHdeUrl(unittest.TestCase):
    """验证详情页访问和写盘逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_visit_hde_url_extracts_imdb_and_writes_release_file(self):
        """详情页包含 IMDb 链接时，应提取 ID 并按规则落盘。"""
        result_item = {
            "title": "Movie / Title: 2026",
            "url": "https://example.com/post",
            "size": "22.4GB",
        }
        response = Mock(text=build_detail_page("https://www.imdb.com/title/tt1234567/"))

        with patch.object(self.module, "get_hde_response", return_value=response) as mock_get, patch.object(
            self.module,
            "normalize_release_title_for_filename",
            return_value="Normalized / Title: 2026",
        ) as mock_normalize, patch.object(
            self.module,
            "sanitize_filename",
            return_value="Sanitized Title 2026",
        ) as mock_sanitize, patch.object(
            self.module,
            "write_list_to_file",
            return_value=True,
        ) as mock_write:
            self.module.visit_hde_url(result_item)

        self.assertEqual(result_item["imdb"], "tt1234567")
        mock_get.assert_called_once_with("https://example.com/post")
        mock_normalize.assert_called_once_with("Movie / Title: 2026")
        mock_sanitize.assert_called_once_with("Normalized / Title: 2026")
        mock_write.assert_called_once_with(
            str(Path(self.module.OUTPUT_DIR) / "Sanitized Title 2026 - hde (22.4GB)[tt1234567].rls"),
            ["https://example.com/post"],
        )

    def test_visit_hde_url_falls_back_to_loose_tt_match_when_imdb_link_is_noncanonical(self):
        """详情页没有标准 IMDb URL 时，仍应从其它链接中回退提取 ``tt`` 编号。"""
        result_item = {
            "title": "Fallback Title",
            "url": "https://example.com/post",
            "size": "1.0GB",
        }
        response = Mock(text=build_detail_page("https://example.com/redirect?target=tt7654321"))

        with patch.object(self.module, "get_hde_response", return_value=response), patch.object(
            self.module,
            "write_list_to_file",
            return_value=True,
        ):
            self.module.visit_hde_url(result_item)

        self.assertEqual(result_item["imdb"], "tt7654321")

    def test_extract_imdb_id_from_links_prefers_canonical_imdb_url(self):
        """同时存在多种链接时，应优先取标准 IMDb 标题页。"""
        imdb_id = self.module.extract_imdb_id_from_links(
            [
                "https://example.com/redirect?target=tt7654321",
                "https://www.imdb.com/title/tt1234567/",
            ]
        )

        self.assertEqual(imdb_id, "tt1234567")

    def test_build_hde_output_filename_uses_sanitized_title_size_and_imdb(self):
        """输出文件名应基于标题、体积和 IMDb 编号拼接。"""
        result_item = {
            "title": "Movie / Title: 2026",
            "size": "22.4GB",
            "imdb": "tt1234567",
        }

        with patch.object(
            self.module,
            "normalize_release_title_for_filename",
            return_value="Normalized / Title: 2026",
        ) as mock_normalize, patch.object(
            self.module,
            "sanitize_filename",
            return_value="Sanitized Title 2026",
        ) as mock_sanitize:
            file_name = self.module.build_hde_output_filename(result_item)

        self.assertEqual(file_name, "Sanitized Title 2026 - hde (22.4GB)[tt1234567].rls")
        mock_normalize.assert_called_once_with("Movie / Title: 2026")
        mock_sanitize.assert_called_once_with("Normalized / Title: 2026")


class TestScrapyHdeMain(unittest.TestCase):
    """验证主流程编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_hde_stops_when_end_title_is_found_on_first_page(self):
        """第一页命中截止标题时应停止翻页。"""
        response = Mock()
        result_list = [{"title": "Old Movie – 1.0 GB", "url": "https://example.com/post-1", "size": "1.0GB"}]

        with patch.object(self.module, "get_hde_response", return_value=response) as mock_get, patch.object(
            self.module,
            "parse_hde_response",
            return_value=result_list,
        ) as mock_parse, patch.object(
            self.module,
            "process_all",
        ) as mock_process:
            self.module.scrapy_hde(start_page=2, end_title="Old Movie – 1.0 GB")

        mock_get.assert_called_once_with("https://example.com/tag/movies/page/2/")
        mock_parse.assert_called_once_with(response)
        mock_process.assert_called_once_with(result_list, max_workers=self.module.DEFAULT_MAX_WORKERS)

    def test_scrapy_hde_moves_to_next_page_until_end_title_is_found(self):
        """未命中截止标题时应继续抓取下一页。"""
        first_response = Mock()
        second_response = Mock()
        first_result_list = [{"title": "New Movie – 2.0 GB", "url": "https://example.com/new", "size": "2.0GB"}]
        second_result_list = [{"title": "Old Movie – 1.0 GB", "url": "https://example.com/old", "size": "1.0GB"}]

        with patch.object(
            self.module,
            "get_hde_response",
            side_effect=[first_response, second_response],
        ) as mock_get, patch.object(
            self.module,
            "parse_hde_response",
            side_effect=[first_result_list, second_result_list],
        ) as mock_parse, patch.object(
            self.module,
            "process_all",
        ) as mock_process:
            self.module.scrapy_hde(start_page=2, end_title="Old Movie – 1.0 GB")

        self.assertEqual(
            mock_get.call_args_list,
            [
                call("https://example.com/tag/movies/page/2/"),
                call("https://example.com/tag/movies/page/3/"),
            ],
        )
        self.assertEqual(mock_parse.call_args_list, [call(first_response), call(second_response)])
        self.assertEqual(
            mock_process.call_args_list,
            [
                call(first_result_list, max_workers=self.module.DEFAULT_MAX_WORKERS),
                call(second_result_list, max_workers=self.module.DEFAULT_MAX_WORKERS),
            ],
        )


if __name__ == "__main__":
    unittest.main()
