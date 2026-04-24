"""
针对 ``my_scripts.scrapy_ttg`` 的单元测试。

这里不依赖真实配置文件，也不会发出真实网络请求。
目标是只验证抓取脚本的核心行为：
1. 单页请求的状态检查与 HTML 解析。
2. ID 过滤、文件名修剪和写盘逻辑。
3. 主入口的分页停止条件、写盘调用和 newest_id 回写。
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

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_ttg.py"


def load_scrapy_ttg(config: dict | None = None):
    """
    在隔离环境中加载 ``scrapy_ttg`` 模块。

    被测模块在 import 时就会读取配置并导入 ``my_module`` / ``retrying``，
    所以这里先注入假的依赖，避免测试依赖本地真实配置和真实重试装饰器。
    """
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "ttg_url": "https://example.com",
        "ttg_movie_url": "https://example.com/browse.php?cat=movie",
        "newest_id": 100,
        "ttg_cookie": "cookie=value",
        "request_head": {"User-Agent": "unit-test"},
        "output_dir": str(Path(temp_dir.name) / "downloads"),
    }
    if config:
        module_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: copy.deepcopy(module_config)
    fake_my_module.sanitize_filename = lambda name: name

    def fake_write_list_to_file(path: str, content: list[str]) -> bool:
        target_path = Path(path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text("\n".join(content), encoding="utf-8")
        return True

    fake_my_module.write_list_to_file = fake_write_list_to_file
    fake_my_module.update_json_config = lambda _file_path, _key, _value: None

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    spec = importlib.util.spec_from_file_location(
        f"scrapy_ttg_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"my_module": fake_my_module, "retrying": fake_retrying}):
        spec.loader.exec_module(module)

    return module, temp_dir


def build_torrent_row(
        torrent_id: str = "123",
        torrent_name: str | None = "Movie Title",
        download_href: str | None = "/download.php?id=123",
        imdb_href: str | None = "https://www.imdb.com/title/tt1234567/",
        size_text: str = "15.2 GB",
) -> str:
    """构造一条最小可用的种子行 HTML。"""
    title_html = ""
    if torrent_name is not None:
        title_html = (
            '<a class="treport">'
            f'<img class="report" torrentname="{torrent_name}" />'
            '</a>'
        )

    download_html = ""
    if download_href is not None:
        download_html = f'<a class="dl_a" href="{download_href}">download</a>'

    imdb_html = ""
    if imdb_href is not None:
        imdb_html = f'<span class="imdb_rate"><a href="{imdb_href}">IMDb</a></span>'

    tds = [
        f"<td>{title_html}</td>",
        f"<td>{download_html}</td>",
        f"<td>{imdb_html}</td>",
        "<td></td>",
        "<td></td>",
        "<td></td>",
        f"<td>{size_text}</td>",
    ]
    return (
        f'<tr class="hover_hr row" id="{torrent_id}">'
        f"{''.join(tds)}"
        "</tr>"
    )


def build_page_html(*rows: str) -> str:
    """把若干种子行拼成最小可用页面。"""
    return f'<table id="torrent_table">{"".join(rows)}</table>'


class TestModuleLoad(unittest.TestCase):
    """验证模块导入时的配置注入。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ttg()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_scrapy_ttg_injects_cookie_into_request_head(self):
        """模块加载时应把配置里的 cookie 注入请求头。"""
        self.assertEqual(self.module.REQUEST_HEAD["Cookie"], "cookie=value")


class TestGetTtgResponse(unittest.TestCase):
    """验证单页请求逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ttg()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_ttg_response_returns_response_and_sets_utf8_encoding(self):
        """请求成功时应返回响应对象，并统一设置 UTF-8 编码。"""
        response = Mock(status_code=200)

        with patch.object(self.module.requests, "get", return_value=response) as mock_get:
            result = self.module.get_ttg_response("https://example.com/page")

        self.assertIs(result, response)
        self.assertEqual(response.encoding, "utf-8")
        mock_get.assert_called_once_with("https://example.com/page", headers=self.module.REQUEST_HEAD)

    def test_get_ttg_response_raises_when_status_code_is_not_200(self):
        """请求返回非 200 状态码时应抛出异常。"""
        response = Mock(status_code=503)

        with patch.object(self.module.requests, "get", return_value=response):
            with self.assertRaisesRegex(Exception, "503"):
                self.module.get_ttg_response("https://example.com/page")


class TestParseTtgResponse(unittest.TestCase):
    """验证 TTG 页面解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ttg()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_ttg_response_returns_structured_rows(self):
        """结构完整的行应被解析成带 URL 和 IMDb 信息的字典。"""
        response = Mock(
            text=build_page_html(
                build_torrent_row(
                    torrent_id="123",
                    torrent_name="Movie Title",
                    download_href="/download.php?id=123",
                    imdb_href="https://www.imdb.com/title/tt7654321/",
                    size_text="15.2 GB",
                )
            )
        )

        result = self.module.parse_ttg_response(response)

        self.assertEqual(
            result,
            [
                {
                    "id": "123",
                    "url": "https://example.com/t/123/",
                    "name": "Movie Title",
                    "dl": "https://example.com/download.php?id=123",
                    "imdb": "tt7654321",
                    "size": "15.2 GB",
                }
            ],
        )

    def test_parse_ttg_response_uses_empty_values_when_optional_nodes_are_missing(self):
        """缺少下载链接、IMDb 链接或标题时，应回退为空字符串。"""
        response = Mock(
            text=build_page_html(
                build_torrent_row(
                    torrent_id="321",
                    torrent_name=None,
                    download_href=None,
                    imdb_href=None,
                    size_text="700 MB",
                )
            )
        )

        result = self.module.parse_ttg_response(response)

        self.assertEqual(
            result,
            [
                {
                    "id": "321",
                    "url": "https://example.com/t/321/",
                    "name": "",
                    "dl": "",
                    "imdb": "",
                    "size": "700 MB",
                }
            ],
        )


class TestFilterAndFixName(unittest.TestCase):
    """验证 ID 过滤和文件名修剪逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ttg({"newest_id": 100})

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_filter_by_id_keeps_only_newer_items(self):
        """应只保留 ID 严格大于 ``NEWEST_ID`` 的项目。"""
        movie_list = [{"id": "99"}, {"id": "100"}, {"id": "101"}, {"id": "205"}]

        result = self.module.filter_by_id(movie_list)

        self.assertEqual(result, [{"id": "101"}, {"id": "205"}])

    def test_fix_name_replaces_path_separators_including_backslash(self):
        """应把斜杠和反斜杠都替换为全角竖线。"""
        result = self.module.fix_name("Title / A \\ B")

        self.assertEqual(result, "Title｜A｜B")

    def test_fix_name_truncates_name_when_exceeding_max_length(self):
        """标题超长时应按上限截断。"""
        long_name = "A" * 230

        result = self.module.fix_name(long_name, max_length=220)

        self.assertEqual(result, "A" * 220)


class TestWriteToDisk(unittest.TestCase):
    """验证落盘逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ttg()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_write_to_disk_creates_output_file_with_expected_content(self):
        """写盘时应创建目录和目标文件，并写入发布页与下载页链接。"""
        self.module.write_to_disk(
            [
                {
                    "name": "Title / A \\ B",
                    "size": "2 GB",
                    "imdb": "tt1234567",
                    "url": "https://example.com/t/123/",
                    "dl": "https://example.com/download.php?id=123",
                }
            ]
        )

        output_dir = Path(self.module.OUTPUT_DIR)
        output_file = output_dir / "Title｜A｜B(2 GB)[tt1234567].ttg"
        self.assertTrue(output_file.exists())
        self.assertEqual(
            output_file.read_text(encoding="utf-8"),
            "https://example.com/t/123/\nhttps://example.com/download.php?id=123",
        )

    def test_write_to_disk_sanitizes_the_full_filename(self):
        """应在完整文件名拼好后再统一交给 ``sanitize_filename`` 清洗。"""
        with patch.object(self.module, "sanitize_filename", return_value="safe-name.ttg") as mock_sanitize:
            self.module.write_to_disk(
                [
                    {
                        "name": "Title / A \\ B",
                        "size": "2:GB",
                        "imdb": "tt1234567",
                        "url": "https://example.com/t/123/",
                        "dl": "https://example.com/download.php?id=123",
                    }
                ]
            )

        mock_sanitize.assert_called_once_with("Title｜A｜B(2:GB)[tt1234567].ttg")
        output_file = Path(self.module.OUTPUT_DIR) / "safe-name.ttg"
        self.assertTrue(output_file.exists())

    def test_write_to_disk_raises_when_write_list_to_file_returns_false(self):
        """底层写盘失败时应显式抛错，而不是静默继续。"""
        with patch.object(self.module, "write_list_to_file", return_value=False):
            with self.assertRaisesRegex(OSError, "写入文件失败"):
                self.module.write_to_disk(
                    [
                        {
                            "name": "Title",
                            "size": "2 GB",
                            "imdb": "tt1234567",
                            "url": "https://example.com/t/123/",
                            "dl": "https://example.com/download.php?id=123",
                        }
                    ]
                )

    def test_write_to_disk_keeps_ttg_suffix_when_filename_is_truncated(self):
        """完整文件名超长时，截断后仍应保留 ``.ttg`` 后缀。"""
        long_name = "A" * 240

        self.module.write_to_disk(
            [
                {
                    "name": long_name,
                    "size": "2 GB",
                    "imdb": "tt1234567",
                    "url": "https://example.com/t/123/",
                    "dl": "https://example.com/download.php?id=123",
                }
            ]
        )

        output_files = list(Path(self.module.OUTPUT_DIR).iterdir())
        self.assertEqual(len(output_files), 1)
        self.assertEqual(output_files[0].suffix, ".ttg")
        self.assertEqual(len(output_files[0].name), 220)


class TestScrapyTtgMain(unittest.TestCase):
    """验证主抓取流程的编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ttg({"newest_id": 100})

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_ttg_stops_immediately_when_first_page_has_no_new_items(self):
        """第一页全部被过滤掉时，应停止抓取且保留原 newest_id。"""
        with patch.object(self.module, "get_ttg_response", return_value=Mock()) as mock_get, patch.object(
            self.module, "parse_ttg_response", return_value=[{"id": "100"}]
        ), patch.object(self.module, "write_to_disk") as mock_write, patch.object(
            self.module, "update_json_config"
        ) as mock_update:
            self.module.scrapy_ttg()

        mock_get.assert_called_once_with("https://example.com/browse.php?cat=movie&&page=0&")
        mock_write.assert_not_called()
        mock_update.assert_called_once_with("config/scrapy_ttg.json", "newest_id", 100)

    def test_scrapy_ttg_writes_each_new_page_and_updates_max_seen_id(self):
        """多页抓取时应逐页写盘，并把最大的新增 ID 写回配置。"""
        parsed_pages = [
            [{"id": "101"}, {"id": "105"}],
            [{"id": "103"}],
            [{"id": "100"}],
        ]

        with patch.object(self.module, "get_ttg_response", side_effect=[Mock(), Mock(), Mock()]) as mock_get, patch.object(
            self.module, "parse_ttg_response", side_effect=parsed_pages
        ), patch.object(self.module, "write_to_disk") as mock_write, patch.object(
            self.module, "update_json_config"
        ) as mock_update:
            self.module.scrapy_ttg()

        self.assertEqual(
            mock_get.call_args_list,
            [
                call("https://example.com/browse.php?cat=movie&&page=0&"),
                call("https://example.com/browse.php?cat=movie&&page=1&"),
                call("https://example.com/browse.php?cat=movie&&page=2&"),
            ],
        )
        self.assertEqual(mock_write.call_args_list, [call([{"id": "101"}, {"id": "105"}]), call([{"id": "103"}])])
        mock_update.assert_called_once_with("config/scrapy_ttg.json", "newest_id", 105)

    def test_scrapy_ttg_does_not_update_newest_id_when_write_to_disk_fails(self):
        """写盘失败时应把异常抛给上层，且不回写 newest_id。"""
        with patch.object(self.module, "get_ttg_response", return_value=Mock()), patch.object(
            self.module, "parse_ttg_response", return_value=[{"id": "101"}]
        ), patch.object(self.module, "write_to_disk", side_effect=OSError("disk full")), patch.object(
            self.module, "update_json_config"
        ) as mock_update:
            with self.assertRaisesRegex(OSError, "disk full"):
                self.module.scrapy_ttg()

        mock_update.assert_not_called()


if __name__ == "__main__":
    unittest.main()
