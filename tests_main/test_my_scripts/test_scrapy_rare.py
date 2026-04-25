"""
针对 ``my_scripts.scrapy_rare`` 的单元测试。

这里在隔离环境中加载模块，不依赖真实配置，也不会发出真实网络请求。
主要验证请求、解析、多线程编排和写盘行为。
"""

import copy
import importlib.util
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from bs4 import BeautifulSoup

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_rare.py"


def load_scrapy_rare(config: dict | None = None):
    """在最小依赖环境中加载 ``scrapy_rare`` 模块。"""
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "request_head": {"User-Agent": "unit-test"},
        "output_dir": str(Path(temp_dir.name) / "downloads"),
    }
    if config:
        module_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: copy.deepcopy(module_config)
    fake_my_module.sanitize_filename = lambda name: name
    fake_my_module.read_file_to_list = lambda path: Path(path).read_text(encoding="utf-8").splitlines()

    def fake_write_list_to_file(path: str, content: list[str]) -> bool:
        target_path = Path(path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text("\n".join(content), encoding="utf-8")
        return True

    fake_my_module.write_list_to_file = fake_write_list_to_file

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    spec = importlib.util.spec_from_file_location(
        f"scrapy_rare_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"my_module": fake_my_module, "retrying": fake_retrying}):
        spec.loader.exec_module(module)

    return module, temp_dir


class TestModuleLoad(unittest.TestCase):
    """验证模块导入时的配置注入。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rare()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_scrapy_rare_uses_injected_config(self):
        """模块加载后应暴露注入的请求头和输出目录。"""
        self.assertEqual(self.module.REQUEST_HEAD, {"User-Agent": "unit-test"})
        self.assertTrue(str(self.module.OUTPUT_DIR).endswith("downloads"))


class TestGetRareResponse(unittest.TestCase):
    """验证单次请求逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rare()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_rare_response_returns_response_and_sets_utf8_encoding(self):
        """请求成功时应返回响应对象，并统一设置 UTF-8 编码。"""
        response = Mock(status_code=200)

        with patch.object(self.module.requests, "get", return_value=response) as mock_get:
            result = self.module.get_rare_response("https://example.com/post")

        self.assertIs(result, response)
        self.assertEqual(response.encoding, "utf-8")
        mock_get.assert_called_once_with("https://example.com/post", headers=self.module.REQUEST_HEAD, timeout=20)

    def test_get_rare_response_raises_when_status_code_is_not_200(self):
        """请求返回非 200 状态码时应抛出异常。"""
        response = Mock(status_code=503)

        with patch.object(self.module.requests, "get", return_value=response):
            with self.assertRaisesRegex(Exception, "503"):
                self.module.get_rare_response("https://example.com/post")

    def test_get_rare_response_propagates_request_exception(self):
        """底层请求异常时应直接抛出，交给重试装饰器处理。"""
        with patch.object(self.module.requests, "get", side_effect=requests.Timeout("timed out")):
            with self.assertRaisesRegex(requests.Timeout, "timed out"):
                self.module.get_rare_response("https://example.com/post")


class TestParseResponse(unittest.TestCase):
    """验证 HTML 解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rare()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_response_collects_entry_content_and_builds_filename(self):
        """应提取正文中的段落、预格式文本和附加链接，并拼出文件名。"""
        response = Mock(
            text="""
            <html>
              <head><title>Rare / Title</title></head>
              <body>
                <div class="entry-content">
                  <p>First <a href="https://example.com/download">Download</a></p>
                  <p><a href="https://example.com/image-link"><img src="cover.jpg" /></a></p>
                  <p>IMDb https://www.imdb.com/title/tt1234567/</p>
                  <pre>CODE 123</pre>
                  <figure><a href="https://example.com/figure">Figure</a></figure>
                  <h4><a href="https://example.com/h4">Header</a></h4>
                </div>
              </body>
            </html>
            """
        )

        with patch.object(self.module, "sanitize_filename", return_value="Rare _ Title") as mock_sanitize:
            result = self.module.parse_response(response)

        self.assertEqual(
            result,
            {
                "file_name": "Rare _ Title[tt1234567].rare",
                "content": "\n".join(
                    [
                        "First Download (https://example.com/download)",
                        "https://example.com/image-link",
                        "IMDb https://www.imdb.com/title/tt1234567/",
                        "CODE 123",
                        "https://example.com/figure",
                        "https://example.com/h4",
                    ]
                ),
            },
        )
        mock_sanitize.assert_called_once_with("Rare / Title")

    def test_parse_response_falls_back_to_div_entry_when_entry_content_missing(self):
        """缺少 ``entry-content`` 时应回退到 ``div.entry``。"""
        response = Mock(
            text="""
            <html>
              <head><title>Fallback</title></head>
              <body>
                <div class="entry">
                  <p>Fallback line</p>
                </div>
              </body>
            </html>
            """
        )

        result = self.module.parse_response(response)

        self.assertEqual(result, {"file_name": "Fallback[].rare", "content": "Fallback line"})

    def test_parse_response_returns_empty_dict_when_entry_container_is_missing(self):
        """页面缺少正文容器时应直接返回空字典。"""
        response = Mock(text="<html><head><title>Ignored</title></head><body><div>blocked</div></body></html>")

        self.assertEqual(self.module.parse_response(response), {})


class TestFindEntry(unittest.TestCase):
    """验证正文容器定位逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rare()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_find_entry_prefers_entry_content_when_both_candidates_exist(self):
        """同时存在两个候选容器时，应优先使用 ``entry-content``。"""
        soup = BeautifulSoup(
            """
            <html>
              <body>
                <div class="entry-content"><p>primary</p></div>
                <div class="entry"><p>fallback</p></div>
              </body>
            </html>
            """,
            "html.parser",
        )

        entry = self.module.find_entry(soup)

        self.assertIsNotNone(entry)
        self.assertEqual(entry.get_text(strip=True), "primary")


class TestExtractEntryLines(unittest.TestCase):
    """验证正文行提取逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rare()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_extract_entry_lines_keeps_bare_link_when_anchor_has_no_text(self):
        """链接只有图片或空文本时，应直接保留裸 URL。"""
        entry = BeautifulSoup(
            """
            <div class="entry-content">
              <p><a href="https://example.com/image-link"><img src="cover.jpg" /></a></p>
            </div>
            """,
            "html.parser",
        ).select_one("div.entry-content")

        result = self.module.extract_entry_lines(entry)

        self.assertEqual(result, ["https://example.com/image-link"])

    def test_extract_entry_lines_flattens_br_and_skips_empty_paragraphs(self):
        """``br`` 应压成空格，空段落和清空后的图片段不应写入结果。"""
        entry = BeautifulSoup(
            """
            <div class="entry-content">
              <p>Alpha<br/>Beta</p>
              <p>   </p>
              <p><img src="only-image.jpg" /></p>
            </div>
            """,
            "html.parser",
        ).select_one("div.entry-content")

        result = self.module.extract_entry_lines(entry)

        self.assertEqual(result, ["Alpha Beta"])


class TestBuildFileName(unittest.TestCase):
    """验证文件名拼装逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rare()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_build_file_name_uses_empty_imdb_placeholder_when_missing(self):
        """正文里缺少 IMDb 时，应保留空占位符。"""
        soup = BeautifulSoup("<html><head><title>No IMDb</title></head></html>", "html.parser")

        result = self.module.build_file_name(soup, "plain text without imdb")

        self.assertEqual(result, "No IMDb[].rare")


class TestProcessAll(unittest.TestCase):
    """验证批量多线程编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rare()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_process_all_runs_all_tasks_without_collecting_results(self):
        """批处理流程只负责执行任务，不再返回无意义的结果列表。"""
        with patch.object(self.module, "visit_rare_url", side_effect=lambda link: f"done:{link}"):
            result = self.module.process_all(["u1", "u2"], max_workers=2)

        self.assertIsNone(result)

    def test_process_all_logs_errors_without_raising(self):
        """单个任务失败时，应记录错误且不向上抛出。"""

        def fake_visit(link: str):
            if link == "bad":
                raise RuntimeError("boom")
            return f"done:{link}"

        with patch.object(self.module, "visit_rare_url", side_effect=fake_visit), self.assertLogs(
            self.module.logger.name,
            level="ERROR",
        ) as logs:
            result = self.module.process_all(["good", "bad"], max_workers=1)

        self.assertIsNone(result)
        self.assertIn("[ERROR] bad -> RuntimeError('boom')", logs.output[0])


class TestVisitRareUrl(unittest.TestCase):
    """验证详情页访问和写盘逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rare()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_visit_rare_url_fetches_parses_and_writes_file(self):
        """访问详情页后应按输出目录拼路径并写入发布页与正文。"""
        response = Mock()

        with patch.object(self.module, "get_rare_response", return_value=response) as mock_get, patch.object(
            self.module,
            "parse_response",
            return_value={"file_name": "sample.rare", "content": "line 1\nline 2"},
        ) as mock_parse, patch.object(self.module, "write_list_to_file", return_value=True) as mock_write:
            result = self.module.visit_rare_url("https://example.com/post")

        self.assertIsNone(result)
        mock_get.assert_called_once_with("https://example.com/post")
        mock_parse.assert_called_once_with(response)
        mock_write.assert_called_once_with(
            str(Path(self.module.OUTPUT_DIR) / "sample.rare"),
            ["https://example.com/post", "line 1\nline 2"],
        )


class TestScrapyRareMain(unittest.TestCase):
    """验证主流程编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rare()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_rare_reads_source_file_and_processes_all_links(self):
        """主入口应读取链接列表，并以固定并发度调用批处理流程。"""
        with patch.object(self.module, "read_file_to_list", return_value=["u1", "u2"]) as mock_read, patch.object(
            self.module,
            "process_all",
        ) as mock_process:
            self.module.scrapy_rare("source.txt")

        mock_read.assert_called_once_with("source.txt")
        mock_process.assert_called_once_with(["u1", "u2"], max_workers=30)


if __name__ == "__main__":
    unittest.main()
