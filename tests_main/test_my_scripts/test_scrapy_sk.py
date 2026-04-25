"""
针对 ``my_scripts.scrapy_sk`` 的单元测试。

这里不依赖真实配置文件，也不会发出真实网络请求。
目标是只验证抓取脚本的核心行为：
1. 单页请求的状态检查与列表 HTML 解析。
2. 详情页 CSFD 信息提取、文件名组装和写盘逻辑。
3. 并发处理和主入口分页停止条件。
"""

import copy
import importlib.util
import re
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, call, patch

import requests

requests.packages.urllib3.disable_warnings()

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_sk.py"


def fake_normalize_release_title_for_filename(
        title: str,
        max_length: int = 220,
        extra_cleanup_patterns=None,
) -> str:
    """最小实现：仅覆盖被测脚本实际依赖到的清理行为。"""
    if extra_cleanup_patterns:
        for pattern in extra_cleanup_patterns:
            title = re.sub(pattern, "", title)

    title = re.sub(r"\s+", " ", title).strip()
    if len(title) <= max_length:
        return title
    return title[:max_length]


def load_scrapy_sk(config: dict | None = None):
    """
    在隔离环境中加载 ``scrapy_sk`` 模块。

    被测模块在 import 时就会读取配置并导入 ``my_module`` /
    ``retrying`` / ``sort_movie_request``，这里先注入假的依赖，
    避免测试依赖本地真实配置和真实网络环境。
    """
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "sk_url": "https://example.com/",
        "sk_movie_url": "https://example.com/browse?page=",
        "sk_cookie": "cookie=value",
        "request_head": {"User-Agent": "unit-test"},
        "output_dir": str(Path(temp_dir.name) / "downloads"),
    }
    if config:
        module_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: copy.deepcopy(module_config)
    fake_my_module.normalize_release_title_for_filename = fake_normalize_release_title_for_filename
    fake_my_module.sanitize_filename = lambda name: name

    def fake_write_list_to_file(path: str, content: list[str]) -> bool:
        target_path = Path(path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text("\n".join(content), encoding="utf-8")
        return True

    fake_my_module.write_list_to_file = fake_write_list_to_file

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    fake_sort_movie_request = types.ModuleType("sort_movie_request")
    fake_sort_movie_request.get_csfd_response = lambda _url: Mock(text="")
    fake_sort_movie_request.get_csfd_movie_details = lambda _response: {
        "origin": "",
        "director": "",
        "id": None,
    }

    spec = importlib.util.spec_from_file_location(
        f"scrapy_sk_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "retrying": fake_retrying,
            "sort_movie_request": fake_sort_movie_request,
        },
    ):
        spec.loader.exec_module(module)

    return module, temp_dir


def build_sk_item_html(
        group: str | None = "Remux",
        group_href: str = "torrents_v2.php?category=1",
        title: str | None = "Movie Title",
        detail_href: str | None = "details.php?name=movie&id=123",
        metadata: str | None = "Velkost 2.4 GB | Pridany 27/07/2025",
) -> str:
    """构造一条最小可用的 SK 列表项 HTML。"""
    parts = []
    if group is not None:
        parts.append(f'<a href="{group_href}">{group}</a>')
    if detail_href is not None:
        parts.append(f'<a href="{detail_href}">{title or ""}</a>')
    if metadata is not None:
        parts.append(f"<div>{metadata}</div>")
    return f'<td class="lista">{"".join(parts)}</td>'


def build_sk_page_html(*items: str) -> str:
    """把若干 SK 列表项拼成最小可用页面。"""
    return f'<table class="lista"><tr><td><table class="lista"><tr>{"".join(items)}</tr></table></td></tr></table>'


def build_detail_page_html(csfd_href: str | None = None) -> str:
    """构造详情页 HTML，可选带上 CSFD 链接。"""
    csfd_html = ""
    if csfd_href is not None:
        csfd_html = (
            f'<a itemprop="sameAs" href="{csfd_href}">'
            '<img src="/torrent/images/csfd.png" />'
            "</a>"
        )
    return f"<html><body>{csfd_html}</body></html>"


class TestModuleLoad(unittest.TestCase):
    """验证模块导入时的配置注入。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_scrapy_sk_injects_cookie_into_request_head(self):
        """模块加载时应把配置里的 cookie 注入请求头。"""
        self.assertEqual(self.module.REQUEST_HEAD["Cookie"], "cookie=value")


class TestGetSkResponse(unittest.TestCase):
    """验证单页请求逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_sk_response_returns_response_and_sets_utf8_encoding(self):
        """请求成功时应返回响应对象，并统一设置 UTF-8 编码。"""
        response = Mock(status_code=200)

        with patch.object(self.module.requests, "get", return_value=response) as mock_get:
            result = self.module.get_sk_response("https://example.com/page")

        self.assertIs(result, response)
        self.assertEqual(response.encoding, "utf-8")
        mock_get.assert_called_once_with("https://example.com/page", headers=self.module.REQUEST_HEAD)

    def test_get_sk_response_raises_when_status_code_is_not_200(self):
        """请求返回非 200 状态码时应抛出异常。"""
        response = Mock(status_code=503)

        with patch.object(self.module.requests, "get", return_value=response):
            with self.assertRaisesRegex(Exception, "503"):
                self.module.get_sk_response("https://example.com/page")

    def test_get_sk_response_propagates_request_exception(self):
        """底层请求异常时应直接抛出，交给重试装饰器处理。"""
        with patch.object(self.module.requests, "get", side_effect=requests.Timeout("timed out")):
            with self.assertRaisesRegex(requests.Timeout, "timed out"):
                self.module.get_sk_response("https://example.com/page")


class TestParseSkResponse(unittest.TestCase):
    """验证 SK 列表页解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_sk_response_returns_structured_items(self):
        """结构完整的列表项应被解析成分组、标题、大小和日期。"""
        response = Mock(
            text=build_sk_page_html(
                build_sk_item_html(
                    group="2160p",
                    title="Movie Title",
                    detail_href="details.php?name=movie&id=321",
                    metadata="Velkost 15.2 GB | Pridany 25/04/2026",
                )
            )
        )

        result = self.module.parse_sk_response(response)

        self.assertEqual(
            result,
            [
                {
                    "group": "2160p",
                    "url": "https://example.com/torrent/details.php?name=movie&id=321",
                    "title": "Movie Title",
                    "size": "15.2 GB",
                    "date": "25/04/2026",
                }
            ],
        )

    def test_parse_sk_response_skips_items_missing_required_fields_or_metadata(self):
        """缺少必要链接或缺少大小日期信息的项都应被跳过。"""
        response = Mock(
            text=build_sk_page_html(
                build_sk_item_html(
                    group="2160p",
                    title="Valid Title",
                    detail_href="details.php?name=movie&id=123",
                    metadata="Velkost 8 GB | Pridany 24/04/2026",
                ),
                build_sk_item_html(
                    group=None,
                    title="Missing Group",
                    detail_href="details.php?name=movie&id=456",
                    metadata="Velkost 10 GB | Pridany 24/04/2026",
                ),
                build_sk_item_html(
                    group="1080p",
                    title="Missing Meta",
                    detail_href="details.php?name=movie&id=789",
                    metadata=None,
                ),
            )
        )

        result = self.module.parse_sk_response(response)

        self.assertEqual(
            result,
            [
                {
                    "group": "2160p",
                    "url": "https://example.com/torrent/details.php?name=movie&id=123",
                    "title": "Valid Title",
                    "size": "8 GB",
                    "date": "24/04/2026",
                }
            ],
        )


class TestVisitSkUrl(unittest.TestCase):
    """验证详情页访问与写盘逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_visit_sk_url_returns_none_when_csfd_link_is_missing(self):
        """详情页没有 CSFD 图标链接时，应直接返回且不做后续请求。"""
        result_item = {
            "title": "Movie Title",
            "size": "4 GB",
            "url": "https://example.com/torrent/details.php?name=movie&id=1",
        }
        response = Mock(text=build_detail_page_html())

        with patch.object(self.module, "get_sk_response", return_value=response), patch.object(
            self.module, "get_csfd_response"
        ) as mock_get_csfd, patch.object(self.module, "get_csfd_movie_details") as mock_get_details, patch.object(
            self.module, "write_list_to_file"
        ) as mock_write:
            result = self.module.visit_sk_url(result_item)

        self.assertIsNone(result)
        mock_get_csfd.assert_not_called()
        mock_get_details.assert_not_called()
        mock_write.assert_not_called()

    def test_visit_sk_url_writes_sk_file_with_normalized_name(self):
        """存在 CSFD 数据时，应按约定生成文件名并写入详情页链接。"""
        result_item = {
            "title": "Movie Title",
            "size": "15.2 GB",
            "url": "https://example.com/torrent/details.php?name=movie&id=321",
        }
        detail_response = Mock(text=build_detail_page_html("https://www.csfd.cz/film/123456"))
        csfd_response = Mock(text="csfd page")
        csfd_data = {
            "origin": "USA = CSFD 88%",
            "director": "Jane Doe",
            "id": "tt1234567",
        }

        with patch.object(self.module, "get_sk_response", return_value=detail_response), patch.object(
            self.module, "get_csfd_response", return_value=csfd_response
        ) as mock_get_csfd, patch.object(
            self.module, "get_csfd_movie_details", return_value=csfd_data
        ) as mock_get_details, patch.object(
            self.module,
            "normalize_release_title_for_filename",
            side_effect=fake_normalize_release_title_for_filename,
        ) as mock_normalize, patch.object(
            self.module,
            "sanitize_filename",
            side_effect=lambda name: name,
        ) as mock_sanitize, patch.object(
            self.module,
            "write_list_to_file",
            return_value=True,
        ) as mock_write:
            self.module.visit_sk_url(result_item)

        mock_get_csfd.assert_called_once_with("https://www.csfd.cz/film/123456")
        mock_get_details.assert_called_once_with(csfd_response)
        mock_normalize.assert_called_once_with(
            "Movie Title#USA = CSFD 88%#{Jane Doe}",
            extra_cleanup_patterns=(r"\s*=\s*CSFD\s*\d+%",),
        )
        mock_sanitize.assert_called_once_with("Movie Title#USA#{Jane Doe}")
        mock_write.assert_called_once_with(
            str(Path(self.module.OUTPUT_DIR) / "Movie Title#USA#{Jane Doe}(15.2 GB)[tt1234567].sk"),
            ["https://example.com/torrent/details.php?name=movie&id=321"],
        )

    def test_visit_sk_url_uses_csfd_fallback_id_when_details_have_no_id(self):
        """CSFD 详情未给出 ID 时，应回退到 CSFD 链接里的编号。"""
        result_item = {
            "title": "Movie Title",
            "size": "6 GB",
            "url": "https://example.com/torrent/details.php?name=movie&id=654",
        }
        detail_response = Mock(text=build_detail_page_html("https://www.csfd.cz/film/654321"))
        csfd_data = {
            "origin": "CZ",
            "director": "Alice",
            "id": None,
        }

        with patch.object(self.module, "get_sk_response", return_value=detail_response), patch.object(
            self.module, "get_csfd_response", return_value=Mock(text="csfd page")
        ), patch.object(
            self.module, "get_csfd_movie_details", return_value=csfd_data
        ), patch.object(
            self.module,
            "write_list_to_file",
            return_value=True,
        ) as mock_write:
            self.module.visit_sk_url(result_item)

        mock_write.assert_called_once_with(
            str(Path(self.module.OUTPUT_DIR) / "Movie Title#CZ#{Alice}(6 GB)[csfd654321].sk"),
            ["https://example.com/torrent/details.php?name=movie&id=654"],
        )


class TestProcessAll(unittest.TestCase):
    """验证并发处理流程。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_process_all_collects_success_results_and_logs_failures(self):
        """单个任务失败时，应记录错误并继续收集其他成功结果。"""
        items = [{"id": "1"}, {"id": "2"}, {"id": "3"}]

        def fake_visit(item):
            if item["id"] == "2":
                raise RuntimeError("boom")
            return f"ok-{item['id']}"

        with patch.object(self.module, "visit_sk_url", side_effect=fake_visit), patch.object(
            self.module.logger, "error"
        ) as mock_error:
            result = self.module.process_all(items, max_workers=2)

        self.assertCountEqual(result, ["ok-1", "ok-3"])
        mock_error.assert_called_once()
        self.assertIn("boom", mock_error.call_args.args[0])
        self.assertIn("'id': '2'", mock_error.call_args.args[0])


class TestScrapySkMain(unittest.TestCase):
    """验证主抓取流程的编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_sk_stops_immediately_when_first_page_contains_end_date(self):
        """第一页命中截止日期时，应立即停止且只处理一页。"""
        parsed_page = [{"date": "15/10/2013"}]

        with patch.object(self.module, "get_sk_response", return_value=Mock()) as mock_get, patch.object(
            self.module, "parse_sk_response", return_value=parsed_page
        ), patch.object(self.module, "process_all") as mock_process:
            self.module.scrapy_sk()

        mock_get.assert_called_once_with("https://example.com/browse?page=0")
        mock_process.assert_called_once_with(parsed_page, max_workers=25)

    def test_scrapy_sk_advances_pages_until_end_date_is_found(self):
        """未命中截止日期时应继续翻页，直到某一页包含截止日期。"""
        parsed_pages = [
            [{"date": "03/05/2026"}],
            [{"date": "02/05/2026"}, {"date": "01/05/2026"}],
        ]

        with patch.object(self.module, "get_sk_response", side_effect=[Mock(), Mock()]) as mock_get, patch.object(
            self.module, "parse_sk_response", side_effect=parsed_pages
        ), patch.object(self.module, "process_all") as mock_process:
            self.module.scrapy_sk(start_page=2, end_data="01/05/2026")

        self.assertEqual(
            mock_get.call_args_list,
            [
                call("https://example.com/browse?page=2"),
                call("https://example.com/browse?page=3"),
            ],
        )
        self.assertEqual(
            mock_process.call_args_list,
            [
                call([{"date": "03/05/2026"}], max_workers=25),
                call([{"date": "02/05/2026"}, {"date": "01/05/2026"}], max_workers=25),
            ],
        )


if __name__ == "__main__":
    unittest.main()
