"""
针对 ``my_scripts.scrapy_mp`` 的单元测试。

这里在隔离环境中加载模块，不依赖真实配置，也不会发出真实网络请求。
主要验证请求、列表页解析、详情页落盘和分页停止条件。
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

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_mp.py"


def load_scrapy_mp(config: dict | None = None):
    """在最小依赖环境中加载 ``scrapy_mp`` 模块。"""
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "mp_url": "https://example.com",
        "mp_movie_url": "https://example.com/movies/page/",
        "mp_cookie": "cookie=value",
        "request_head": {"User-Agent": "unit-test"},
        "output_dir": temp_dir.name,
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
        f"scrapy_mp_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"my_module": fake_my_module, "retrying": fake_retrying}):
        spec.loader.exec_module(module)

    return module, temp_dir


def build_archive_article(title: str = "Movie Title", link: str = "https://example.com/post", span_text: str = "Jul. 20, 1990") -> str:
    """构造一个最小可用的列表页条目。"""
    return f"""
    <article class="item movies">
      <div class="data">
        <h3><a href="{link}">{title}</a></h3>
        <span>{span_text}</span>
      </div>
    </article>
    """


def build_archive_page(*articles: str) -> str:
    """构造一个最小可用的列表页 HTML。"""
    return f'<div id="archive-content">{"".join(articles)}</div>'


def build_detail_page(id_links: list[str] | None = None, description_html: str | None = None) -> str:
    """构造一个最小可用的详情页 HTML。"""
    if id_links is None:
        id_links = ['https://www.imdb.com/title/tt1234567/']

    custom_fields_html = ""
    if id_links is not None:
        anchors = "".join(f'<a href="{href}">id</a>' for href in id_links)
        custom_fields_html = f'<div class="custom_fields2">{anchors}</div>'

    desc_html = ""
    if description_html is not None:
        desc_html = f'<div itemprop="description" class="wp-content">{description_html}</div>'

    return f"<html><body>{custom_fields_html}{desc_html}</body></html>"


class TestModuleLoad(unittest.TestCase):
    """验证模块导入时的配置注入。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_scrapy_mp_injects_cookie_into_request_head(self):
        """模块加载时应把配置里的 cookie 注入请求头。"""
        self.assertEqual(self.module.REQUEST_HEAD["Cookie"], "cookie=value")


class TestGetMpResponse(unittest.TestCase):
    """验证单次请求逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_mp_response_returns_response_and_sets_utf8_encoding(self):
        """请求成功时应返回响应对象，并统一设置 UTF-8 编码。"""
        response = Mock(status_code=200)

        with patch.object(self.module.requests, "get", return_value=response) as mock_get:
            result = self.module.get_mp_response("https://example.com/post")

        self.assertIs(result, response)
        self.assertEqual(response.encoding, "utf-8")
        mock_get.assert_called_once_with("https://example.com/post", headers=self.module.REQUEST_HEAD)

    def test_get_mp_response_raises_when_status_code_is_not_200(self):
        """请求返回非 200 状态码时应抛出异常。"""
        response = Mock(status_code=503)

        with patch.object(self.module.requests, "get", return_value=response):
            with self.assertRaisesRegex(Exception, "503"):
                self.module.get_mp_response("https://example.com/post")

    def test_get_mp_response_propagates_request_exception(self):
        """底层请求异常时应直接抛出，交给重试装饰器处理。"""
        with patch.object(self.module.requests, "get", side_effect=requests.Timeout("timed out")):
            with self.assertRaisesRegex(requests.Timeout, "timed out"):
                self.module.get_mp_response("https://example.com/post")


class TestParseMpResponse(unittest.TestCase):
    """验证列表页解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_mp_response_extracts_title_link_and_year(self):
        """应从列表页提取标题、链接和年份。"""
        response = Mock(
            text=build_archive_page(
                build_archive_article(
                    title="Movie Title",
                    link="https://example.com/post-1",
                    span_text="Jul. 20, 1990",
                )
            )
        )

        result = self.module.parse_mp_response(response)

        self.assertEqual(
            result,
            [
                {
                    "title": "Movie Title",
                    "link": "https://example.com/post-1",
                    "year": "1990",
                }
            ],
        )

    def test_parse_mp_response_returns_empty_list_when_archive_content_is_missing(self):
        """页面缺少 ``archive-content`` 时应返回空列表。"""
        response = Mock(text="<html><body><div>blocked</div></body></html>")

        self.assertEqual(self.module.parse_mp_response(response), [])

    def test_parse_mp_response_skips_entries_without_h3_anchor(self):
        """条目缺少标题链接时应跳过，而不是抛异常。"""
        response = Mock(
            text=build_archive_page(
                '<article class="item movies"><div class="data"><span>1990</span></div></article>',
                build_archive_article(
                    title="Valid Movie",
                    link="https://example.com/post-2",
                    span_text="1991",
                ),
            )
        )

        result = self.module.parse_mp_response(response)

        self.assertEqual(
            result,
            [
                {
                    "title": "Valid Movie",
                    "link": "https://example.com/post-2",
                    "year": "1991",
                }
            ],
        )


class TestProcessAll(unittest.TestCase):
    """验证批量多线程编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_process_all_collects_successful_results(self):
        """应收集所有成功任务的返回值。"""

        def fake_visit(item: dict):
            return f"done:{item['link']}"

        with patch.object(self.module, "visit_mp_url", side_effect=fake_visit):
            result = self.module.process_all(
                [{"link": "u1"}, {"link": "u2"}],
                max_workers=2,
            )

        self.assertCountEqual(result, ["done:u1", "done:u2"])

    def test_process_all_logs_errors_without_raising(self):
        """单个任务失败时，应记录错误且继续处理后续任务。"""

        def fake_visit(item: dict):
            if item["link"] == "bad":
                raise RuntimeError("boom")
            return f"done:{item['link']}"

        with patch.object(self.module, "visit_mp_url", side_effect=fake_visit), self.assertLogs(
            self.module.logger.name,
            level="ERROR",
        ) as logs:
            result = self.module.process_all(
                [{"link": "good"}, {"link": "bad"}],
                max_workers=1,
            )

        self.assertEqual(result, ["done:good"])
        self.assertIn("[ERROR] {'link': 'bad'} -> RuntimeError('boom')", logs.output[0])


class TestParseMpDetail(unittest.TestCase):
    """验证详情页解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_mp_detail_extracts_imdb_id_and_builds_output_payload(self):
        """详情页存在 IMDb 和正文时，应完成文本替换并返回输出载荷。"""
        response = Mock(
            text=build_detail_page(
                id_links=["https://www.imdb.com/title/tt1234567/"],
                description_html=(
                    '<p><a href="https://example.com/download">Download</a></p>'
                    '<p><a href="https://example.com/plain"></a></p>'
                ),
            )
        )

        with patch.object(self.module, "get_mp_response", return_value=response) as mock_get, patch.object(
            self.module,
            "normalize_release_title_for_filename",
            return_value="Movie｜Title",
        ) as mock_normalize, patch.object(
            self.module,
            "sanitize_filename",
            return_value="Safe Title",
        ) as mock_sanitize:
            result = self.module.parse_mp_detail(
                response,
                {
                    "title": "Movie / Title",
                    "link": "https://example.com/post",
                    "year": "1990",
                },
            )

        self.assertEqual(
            result,
            {
                "file_name": "Safe Title(1990) - mp [tt1234567].rare",
                "content": "Download (https://example.com/download)\nhttps://example.com/plain",
            },
        )
        mock_normalize.assert_called_once_with("Movie / Title")
        mock_sanitize.assert_called_once_with("Movie｜Title")

    def test_parse_mp_detail_falls_back_to_tmdb_id_when_imdb_is_missing(self):
        """没有 IMDb 时应使用 TMDb 编号作为文件后缀。"""
        response = Mock(
            text=build_detail_page(
                id_links=["https://www.themoviedb.org/movie/98765-sample"],
                description_html="<p>Plain text</p>",
            )
        )

        result = self.module.parse_mp_detail(
            response,
            {
                "title": "Movie Title",
                "link": "https://example.com/post",
                "year": "1990",
            },
        )

        self.assertEqual(
            result,
            {
                "file_name": "Movie Title(1990) - mp [tmdb98765].rare",
                "content": "Plain text",
            },
        )

    def test_parse_mp_detail_returns_none_when_custom_fields_are_missing(self):
        """缺少编号区块时应返回 ``None``。"""
        response = Mock(text='<html><body><div itemprop="description" class="wp-content"><p>Body</p></div></body></html>')

        result = self.module.parse_mp_detail(
            response,
            {
                "title": "Movie Title",
                "link": "https://example.com/post",
                "year": "1990",
            },
        )

        self.assertIsNone(result)

    def test_parse_mp_detail_returns_empty_string_when_description_is_missing(self):
        """缺少正文区块时应返回空字符串。"""
        response = Mock(text=build_detail_page(id_links=["https://www.imdb.com/title/tt1234567/"], description_html=None))

        result = self.module.parse_mp_detail(
            response,
            {
                "title": "Movie Title",
                "link": "https://example.com/post",
                "year": "1990",
            },
        )

        self.assertEqual(result, "")


class TestVisitMpUrl(unittest.TestCase):
    """验证详情页访问和写盘逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_visit_mp_url_requests_page_parses_and_writes_file(self):
        """访问详情页后应委托解析，并把结果写入输出目录。"""
        response = Mock()
        result_item = {
            "title": "Movie Title",
            "link": "https://example.com/post",
            "year": "1990",
        }

        with patch.object(self.module, "get_mp_response", return_value=response) as mock_get, patch.object(
            self.module,
            "parse_mp_detail",
            return_value={"file_name": "sample.rare", "content": "line 1\nline 2"},
        ) as mock_parse, patch.object(
            self.module,
            "write_list_to_file",
            return_value=True,
        ) as mock_write:
            result = self.module.visit_mp_url(result_item)

        self.assertIsNone(result)
        mock_get.assert_called_once_with("https://example.com/post")
        mock_parse.assert_called_once_with(response, result_item)
        mock_write.assert_called_once_with(
            str(Path(self.module.OUTPUT_DIR) / "sample.rare"),
            ["https://example.com/post", "line 1\nline 2"],
        )

    def test_visit_mp_url_returns_parse_result_when_detail_is_not_dict(self):
        """解析失败时应直接透传返回值，并跳过写盘。"""
        response = Mock()
        result_item = {
            "title": "Movie Title",
            "link": "https://example.com/post",
            "year": "1990",
        }

        with patch.object(self.module, "get_mp_response", return_value=response), patch.object(
            self.module,
            "parse_mp_detail",
            return_value="",
        ) as mock_parse, patch.object(
            self.module,
            "write_list_to_file",
        ) as mock_write:
            result = self.module.visit_mp_url(result_item)

        self.assertEqual(result, "")
        mock_parse.assert_called_once_with(response, result_item)
        mock_write.assert_not_called()


class TestScrapyMpMain(unittest.TestCase):
    """验证主抓取流程的编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_mp_stops_when_all_end_files_exist(self):
        """停止条件文件全部出现后，应结束翻页。"""
        sentinel_files = ["end-1.rare", "end-2.rare"]
        process_calls = []

        def fake_process(_result_list, max_workers: int):
            process_calls.append(max_workers)
            if len(process_calls) == 2:
                for file_name in sentinel_files:
                    Path(self.module.OUTPUT_DIR, file_name).write_text("done", encoding="utf-8")

        with patch.object(self.module, "get_mp_response", side_effect=[Mock(), Mock()]) as mock_get, patch.object(
            self.module,
            "parse_mp_response",
            side_effect=[[{"link": "u1"}], [{"link": "u2"}]],
        ), patch.object(self.module, "process_all", side_effect=fake_process) as mock_process:
            self.module.scrapy_mp(start_page=2, end=sentinel_files)

        self.assertEqual(
            mock_get.call_args_list,
            [
                call("https://example.com/movies/page/2/"),
                call("https://example.com/movies/page/3/"),
            ],
        )
        self.assertEqual(mock_process.call_args_list, [call([{"link": "u1"}], max_workers=20), call([{"link": "u2"}], max_workers=20)])

    def test_scrapy_mp_stops_when_explicit_single_end_file_exists(self):
        """显式传入单文件列表且文件已存在时，应在第一页后直接停止。"""
        sentinel_file = Path(self.module.OUTPUT_DIR) / "face-to-face-2"
        sentinel_file.write_text("done", encoding="utf-8")

        with patch.object(
            self.module,
            "get_mp_response",
            side_effect=[Mock(), RuntimeError("should not request second page")],
        ) as mock_get, patch.object(
            self.module,
            "parse_mp_response",
            return_value=[],
        ), patch.object(
            self.module,
            "process_all",
        ) as mock_process:
            self.module.scrapy_mp(start_page=0, end=["face-to-face-2"])

        mock_get.assert_called_once_with("https://example.com/movies/page/0/")
        mock_process.assert_called_once_with([], max_workers=20)


if __name__ == "__main__":
    unittest.main()
