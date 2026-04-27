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
from bs4 import BeautifulSoup
try:
    import fakeredis
except ImportError:  # pragma: no cover - 由依赖安装状态决定
    fakeredis = None

requests.packages.urllib3.disable_warnings()

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_sk.py"
REDIS_HELPER_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_redis.py"


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
        "thread_number": 2,
        "max_empty_pages": 5,
        "excluded_groups": ["Knihy a Časopisy"],
        "end_data": "15/10/2013",
        "redis_pending_key": "sk_pending",
        "redis_processing_key": "sk_processing",
        "redis_failed_key": "sk_failed",
        "redis_seen_key": "sk_seen",
        "redis_scan_page_key": "sk_scan_page",
        "redis_scan_complete_key": "sk_scan_complete",
        "redis_next_end_data_key": "sk_next_end_data",
    }
    helper_config = {
        "redis_host": "127.0.0.1",
        "redis_port": 6379,
        "redis_db": 0,
    }
    if config:
        module_config.update(config)
        for key in ("redis_host", "redis_port", "redis_db"):
            if key in config:
                helper_config[key] = config[key]

    fake_my_module = types.ModuleType("my_module")

    def fake_read_json_to_dict(path: str):
        if path == "config/scrapy_redis.json":
            return copy.deepcopy(helper_config)
        return copy.deepcopy(module_config)

    fake_my_module.read_json_to_dict = fake_read_json_to_dict
    fake_my_module.normalize_release_title_for_filename = fake_normalize_release_title_for_filename
    fake_my_module.sanitize_filename = lambda name: name
    fake_my_module.update_json_config = lambda _path, _key, _value: None

    def fake_write_list_to_file(path: str, content: list[str]) -> bool:
        target_path = Path(path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text("\n".join(content), encoding="utf-8")
        return True

    fake_my_module.write_list_to_file = fake_write_list_to_file

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    fake_redis = types.ModuleType("redis")

    class DummyRedis:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    fake_redis.Redis = DummyRedis

    fake_sort_movie_request = types.ModuleType("sort_movie_request")
    fake_sort_movie_request.get_csfd_response = lambda _url: Mock(text="")
    fake_sort_movie_request.get_csfd_movie_details = lambda _response: {
        "origin": "",
        "director": "",
        "id": None,
    }

    helper_spec = importlib.util.spec_from_file_location(
        f"scrapy_redis_test_{uuid.uuid4().hex}",
        REDIS_HELPER_PATH,
    )
    helper_module = importlib.util.module_from_spec(helper_spec)
    with patch.dict(sys.modules, {"my_module": fake_my_module, "redis": fake_redis}):
        helper_spec.loader.exec_module(helper_module)

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
            "redis": fake_redis,
            "scrapy_redis": helper_module,
            "sort_movie_request": fake_sort_movie_request,
        },
    ):
        spec.loader.exec_module(module)

    module._scrapy_redis = helper_module
    return module, temp_dir


class _FallbackFakeRedisPipeline:
    """最小 Redis pipeline 实现。"""

    def __init__(self, client):
        self.client = client
        self.commands = []

    def sadd(self, key: str, value: str):
        self.commands.append(("sadd", key, value))
        return self

    def rpush(self, key: str, value: str):
        self.commands.append(("rpush", key, value))
        return self

    def execute(self):
        results = []
        for command, key, value in self.commands:
            results.append(getattr(self.client, command)(key, value))
        self.commands.clear()
        return results


class _FallbackFakeRedis:
    """fakeredis 不可用时使用的最小内存 Redis 实现。"""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.sets = {}
        self.lists = {}
        self.values = {}

    def pipeline(self):
        return _FallbackFakeRedisPipeline(self)

    def sadd(self, key: str, value: str) -> int:
        members = self.sets.setdefault(key, set())
        if value in members:
            return 0
        members.add(value)
        return 1

    def rpush(self, key: str, value: str) -> int:
        items = self.lists.setdefault(key, [])
        items.append(value)
        return len(items)

    def rpoplpush(self, source: str, destination: str):
        source_items = self.lists.setdefault(source, [])
        if not source_items:
            return None
        value = source_items.pop()
        self.lists.setdefault(destination, []).insert(0, value)
        return value

    def lrem(self, key: str, count: int, value: str) -> int:
        items = self.lists.setdefault(key, [])
        removed = 0
        new_items = []
        for item in items:
            if item == value and removed < count:
                removed += 1
                continue
            new_items.append(item)
        self.lists[key] = new_items
        return removed

    def llen(self, key: str) -> int:
        return len(self.lists.get(key, []))

    def lrange(self, key: str, start: int, end: int) -> list[str]:
        items = self.lists.get(key, [])
        if end == -1:
            end = len(items) - 1
        return items[start:end + 1]

    def eval(self, _script: str, numkeys: int, *keys_and_args):
        keys = keys_and_args[:numkeys]
        args = keys_and_args[numkeys:]
        if len(keys) != 2:
            raise AssertionError("expected seen_key and pending_key")
        if len(args) % 2 != 0:
            raise AssertionError("expected alternating unique_value/payload args")

        seen_key, pending_key = keys
        enqueued = 0
        for index in range(0, len(args), 2):
            unique_value = args[index]
            payload = args[index + 1]
            if self.sadd(seen_key, unique_value):
                self.rpush(pending_key, payload)
                enqueued += 1
        return enqueued

    def get(self, key: str):
        return self.values.get(key)

    def set(self, key: str, value: str):
        self.values[key] = value
        return True

    def delete(self, *keys: str) -> int:
        deleted = 0
        for key in keys:
            if key in self.values:
                del self.values[key]
                deleted += 1
            if key in self.lists:
                del self.lists[key]
                deleted += 1
            if key in self.sets:
                del self.sets[key]
                deleted += 1
        return deleted


if fakeredis is None:
    class FakeRedis(_FallbackFakeRedis):
        """回退到手写内存 Redis。"""

else:
    class FakeRedis(fakeredis.FakeRedis):
        """优先使用带真实命令语义的 fakeredis。"""

        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            super().__init__(decode_responses=True)


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


def build_sk_filtered_empty_page_html() -> str:
    """构造账户过滤导致的 SK 空结果页。"""
    notice_row = (
        '<td class="lista" align="center" colspan="16">'
        '<a href="index.php">Nenasli ste co ste hladali???...Napiste nam to na nastenku</a><br/>'
        '</td>'
    )
    return build_sk_page_html(notice_row)


class TestModuleLoad(unittest.TestCase):
    """验证模块导入时的配置注入。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_scrapy_sk_injects_cookie_into_request_head(self):
        """模块加载时应把配置里的 cookie 注入请求头。"""
        self.assertEqual(self.module.REQUEST_HEAD["Cookie"], "cookie=value")

    def test_load_scrapy_sk_reads_end_data_from_config(self):
        """模块加载时应读取配置里的截止日期。"""
        self.assertEqual(self.module.END_DATA, "15/10/2013")


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
        mock_get.assert_called_once_with(
            "https://example.com/page",
            headers=self.module.REQUEST_HEAD,
            timeout=20,
        )

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


class TestSkRowLinkHelpers(unittest.TestCase):
    """验证 SK 行内链接提取 helper。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_extract_sk_row_links_returns_structured_data(self):
        """结构完整的列表项应返回分组、标题和详情页链接。"""
        row = BeautifulSoup(
            build_sk_page_html(
                build_sk_item_html(
                    group="2160p",
                    title="Movie Title",
                    detail_href="details.php?name=movie&id=321",
                    metadata="Velkost 15.2 GB | Pridany 25/04/2026",
                )
            ),
            "html.parser",
        ).find("td", class_="lista")

        result = self.module.extract_sk_row_links(row)

        self.assertEqual(
            result,
            {
                "group": "2160p",
                "url": "https://example.com/torrent/details.php?name=movie&id=321",
                "title": "Movie Title",
            },
        )

    def test_extract_sk_row_links_returns_none_when_required_links_are_missing(self):
        """缺少关键链接字段时，应返回 ``None``。"""
        row = BeautifulSoup(
            build_sk_page_html(
                build_sk_item_html(
                    group=None,
                    title="Missing Group",
                    detail_href="details.php?name=movie&id=456",
                    metadata="Velkost 10 GB | Pridany 24/04/2026",
                )
            ),
            "html.parser",
        ).find("td", class_="lista")

        self.assertIsNone(self.module.extract_sk_row_links(row))


class TestParseSkRow(unittest.TestCase):
    """验证 SK 单行解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_sk_row_returns_structured_item(self):
        """结构完整的列表项应被解析成分组、标题、大小和日期。"""
        row = BeautifulSoup(
            build_sk_page_html(
                build_sk_item_html(
                    group="2160p",
                    title="Movie Title",
                    detail_href="details.php?name=movie&id=321",
                    metadata="Velkost 15.2 GB | Pridany 25/04/2026",
                )
            ),
            "html.parser",
        ).find("td", class_="lista")

        result = self.module.parse_sk_row(row)

        self.assertEqual(
            result,
            {
                "group": "2160p",
                "url": "https://example.com/torrent/details.php?name=movie&id=321",
                "title": "Movie Title",
                "size": "15.2 GB",
                "date": "25/04/2026",
            },
        )

    def test_parse_sk_row_logs_specific_message_when_links_are_missing(self):
        """缺少链接字段时，应记录链接字段缺失日志。"""
        row = BeautifulSoup(
            build_sk_page_html(
                build_sk_item_html(
                    group=None,
                    title="Missing Group",
                    detail_href="details.php?name=movie&id=456",
                    metadata="Velkost 10 GB | Pridany 24/04/2026",
                )
            ),
            "html.parser",
        ).find("td", class_="lista")

        with patch.object(self.module.logger, "info") as mock_info:
            result = self.module.parse_sk_row(row)

        self.assertIsNone(result)
        mock_info.assert_called_once()
        message, group, url, title, td_snippet = mock_info.call_args[0]
        self.assertEqual(message, "跳过：缺少链接字段 - group=%r url=%r title=%r td=%s")
        self.assertIsNone(group)
        self.assertEqual(url, "https://example.com/torrent/details.php?name=movie&id=456")
        self.assertEqual(title, "Missing Group")
        self.assertIn("details.php?name=movie&amp;id=456", td_snippet)
        self.assertIn("Missing Group", td_snippet)

    def test_parse_sk_row_returns_none_when_size_or_date_are_missing(self):
        """缺少大小日期信息时，应返回 ``None``。"""
        row = BeautifulSoup(
            build_sk_page_html(
                build_sk_item_html(
                    group="1080p",
                    title="Missing Meta",
                    detail_href="details.php?name=movie&id=789",
                    metadata=None,
                )
            ),
            "html.parser",
        ).find("td", class_="lista")

        with patch.object(self.module.logger, "info") as mock_info:
            result = self.module.parse_sk_row(row)

        self.assertIsNone(result)
        mock_info.assert_called_once_with(
            "跳过：缺少大小日期字段 - Missing Meta - https://example.com/torrent/details.php?name=movie&id=789"
        )

    def test_parse_sk_row_returns_none_when_group_is_excluded(self):
        """排除分组的帖子不应进入后续抓取流程。"""
        row = BeautifulSoup(
            build_sk_page_html(
                build_sk_item_html(
                    group="Knihy a Časopisy",
                    group_href="torrents_v2.php?category=23",
                    title="Computer (04/2026) (CZ)",
                    detail_href="details.php?name=computer&id=999",
                    metadata="Velkost 79.3 MB | Pridany 25/03/2026",
                )
            ),
            "html.parser",
        ).find("td", class_="lista")

        self.assertIsNone(self.module.parse_sk_row(row))


class TestSkRowMetadataHelpers(unittest.TestCase):
    """验证 SK 行内元数据提取 helper。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_extract_sk_row_size_date_accepts_spacing_variants(self):
        """大小日期文本中的空格不规则时，仍应正确提取。"""
        row = BeautifulSoup(
            build_sk_page_html(
                build_sk_item_html(
                    metadata="Velkost   2.4 GB|Pridany  27/07/2025",
                )
            ),
            "html.parser",
        ).find("td", class_="lista")

        result = self.module.extract_sk_row_size_date(row)

        self.assertEqual(
            result,
            {
                "size": "2.4 GB",
                "date": "27/07/2025",
            },
        )

    def test_extract_sk_row_size_date_returns_none_when_text_format_is_invalid(self):
        """元数据存在但分隔格式不对时，应返回 ``None`` 而不是抛异常。"""
        row = BeautifulSoup(
            build_sk_page_html(
                build_sk_item_html(
                    metadata="Velkost 2.4 GB Pridany 27/07/2025",
                )
            ),
            "html.parser",
        ).find("td", class_="lista")

        self.assertIsNone(self.module.extract_sk_row_size_date(row))


class TestParseSkResponse(unittest.TestCase):
    """验证 SK 列表页解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_sk_response_collects_only_valid_rows(self):
        """整页解析时应收集有效行并跳过无效行。"""
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

    def test_parse_sk_response_passes_each_row_to_parse_sk_row(self):
        """整页解析应逐行委托给 ``parse_sk_row``。"""
        response = Mock(
            text=build_sk_page_html(
                build_sk_item_html(title="First"),
                build_sk_item_html(title="Second", detail_href="details.php?name=movie&id=456"),
            )
        )

        with patch.object(
            self.module,
            "parse_sk_row",
            side_effect=[
                {"title": "First"},
                None,
            ],
        ) as mock_parse_row:
            result = self.module.parse_sk_response(response)

        self.assertEqual(result, [{"title": "First"}])
        self.assertEqual(mock_parse_row.call_count, 2)

    def test_parse_sk_response_raises_when_no_rows_are_found(self):
        """页面里找不到任何列表项时，应显式抛错而不是静默返回空列表。"""
        response = Mock(text="<html><body><div>empty</div></body></html>")

        with self.assertRaisesRegex(RuntimeError, "网站结构可能已变更"):
            self.module.parse_sk_response(response)

    def test_parse_sk_response_returns_empty_list_for_filtered_empty_page(self):
        """账户过滤导致的已知空页应返回空列表，而不是误报结构异常。"""
        response = Mock(text=build_sk_filtered_empty_page_html())

        self.assertEqual(self.module.parse_sk_response(response), [])

    def test_parse_sk_response_skips_excluded_groups_and_keeps_movies(self):
        """混合页面里应排除书籍分组，仅保留电影帖子。"""
        response = Mock(
            text=build_sk_page_html(
                build_sk_item_html(
                    group="Knihy a Časopisy",
                    group_href="torrents_v2.php?category=23",
                    title="Computer (04/2026) (CZ)",
                    detail_href="details.php?name=computer&id=999",
                    metadata="Velkost 79.3 MB | Pridany 25/03/2026",
                ),
                build_sk_item_html(
                    group="2160p",
                    title="Valid Title",
                    detail_href="details.php?name=movie&id=123",
                    metadata="Velkost 8 GB | Pridany 24/04/2026",
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


class TestSkDetailHelpers(unittest.TestCase):
    """验证 SK 详情页相关 helper。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_extract_csfd_url_from_sk_detail_returns_href(self):
        """详情页存在 CSFD 图标链接时，应提取其 href。"""
        result = self.module.extract_csfd_url_from_sk_detail(
            build_detail_page_html("https://www.csfd.cz/film/123456")
        )

        self.assertEqual(result, "https://www.csfd.cz/film/123456")

    def test_extract_csfd_url_from_sk_detail_returns_none_when_missing(self):
        """详情页没有 CSFD 图标链接时，应返回 ``None``。"""
        self.assertIsNone(self.module.extract_csfd_url_from_sk_detail(build_detail_page_html()))

    def test_get_normalized_csfd_data_keeps_existing_id_and_fills_missing_text_fields(self):
        """已有 IMDb/CSFD ID 时，应保留该 ID，并为缺失文本字段回填空串。"""
        csfd_response = Mock(text="csfd page")

        with patch.object(self.module, "get_csfd_response", return_value=csfd_response) as mock_get_response, patch.object(
            self.module, "get_csfd_movie_details", return_value={"id": "tt1234567"}
        ) as mock_get_details:
            result = self.module.get_normalized_csfd_data("https://www.csfd.cz/film/123456")

        self.assertEqual(
            result,
            {
                "origin": "",
                "director": "",
                "id": "tt1234567",
            },
        )
        mock_get_response.assert_called_once_with("https://www.csfd.cz/film/123456")
        mock_get_details.assert_called_once_with(csfd_response)

    def test_get_normalized_csfd_data_uses_csfd_fallback_id_with_trailing_slash(self):
        """缺少 ID 时，应从 CSFD 链接中稳定提取编号，忽略尾部斜杠。"""
        with patch.object(self.module, "get_csfd_response", return_value=Mock(text="csfd page")), patch.object(
            self.module, "get_csfd_movie_details", return_value={"origin": "CZ", "director": "Alice", "id": None}
        ):
            result = self.module.get_normalized_csfd_data("https://www.csfd.cz/film/654321/")

        self.assertEqual(
            result,
            {
                "origin": "CZ",
                "director": "Alice",
                "id": "csfd654321",
            },
        )

    def test_get_normalized_csfd_data_uses_empty_defaults_when_details_return_none(self):
        """详情解析直接返回 ``None`` 时，应回退到空字段和 CSFD fallback ID。"""
        with patch.object(self.module, "get_csfd_response", return_value=Mock(text="csfd page")), patch.object(
            self.module, "get_csfd_movie_details", return_value=None
        ):
            result = self.module.get_normalized_csfd_data("https://www.csfd.cz/film/777888")

        self.assertEqual(
            result,
            {
                "origin": "",
                "director": "",
                "id": "csfd777888",
            },
        )

    def test_build_sk_output_filename_applies_normalization_and_suffix(self):
        """输出文件名应包含清理后的主体、大小、ID 和 ``.sk`` 后缀。"""
        result_item = {
            "title": "Movie Title",
            "size": "15.2 GB",
            "url": "https://example.com/torrent/details.php?name=movie&id=321",
        }
        csfd_data = {
            "origin": "USA = CSFD 88%",
            "director": "Jane Doe",
            "id": "tt1234567",
        }

        with patch.object(
            self.module,
            "normalize_release_title_for_filename",
            side_effect=fake_normalize_release_title_for_filename,
        ) as mock_normalize, patch.object(
            self.module,
            "sanitize_filename",
            side_effect=lambda name: name,
        ) as mock_sanitize, patch.object(
            self.module.logger,
            "info",
        ):
            result = self.module.build_sk_output_filename(result_item, csfd_data)

        mock_normalize.assert_called_once_with(
            "Movie Title#USA = CSFD 88%#{Jane Doe}",
            extra_cleanup_patterns=(r"\s*=\s*CSFD\s*\d+%",),
        )
        mock_sanitize.assert_called_once_with("Movie Title#USA#{Jane Doe}")
        self.assertEqual(result, "Movie Title#USA#{Jane Doe}(15.2 GB)[tt1234567].sk")


class TestVisitSkUrl(unittest.TestCase):
    """验证详情页访问与写盘编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_visit_sk_url_raises_when_csfd_link_is_missing(self):
        """详情页没有 CSFD 图标链接时，应抛错交给失败队列处理。"""
        result_item = {
            "title": "Movie Title",
            "size": "4 GB",
            "url": "https://example.com/torrent/details.php?name=movie&id=1",
        }
        response = Mock(text=build_detail_page_html())

        with patch.object(self.module, "get_sk_response", return_value=response), patch.object(
            self.module, "extract_csfd_url_from_sk_detail", return_value=None
        ) as mock_extract, patch.object(self.module, "get_normalized_csfd_data") as mock_get_csfd_data, patch.object(
            self.module, "build_sk_output_filename"
        ) as mock_build_name, patch.object(
            self.module, "write_list_to_file"
        ) as mock_write:
            with self.assertRaisesRegex(RuntimeError, "未找到 CSFD 链接"):
                self.module.visit_sk_url(result_item)

        mock_extract.assert_called_once_with(response.text)
        mock_get_csfd_data.assert_not_called()
        mock_build_name.assert_not_called()
        mock_write.assert_not_called()

    def test_visit_sk_url_delegates_to_helpers_and_writes_detail_url(self):
        """主流程应串联 helper，并把详情页链接写入最终文件。"""
        result_item = {
            "title": "Movie Title",
            "size": "6 GB",
            "url": "https://example.com/torrent/details.php?name=movie&id=654",
        }
        detail_response = Mock(text=build_detail_page_html("https://www.csfd.cz/film/654321"))
        csfd_data = {"origin": "CZ", "director": "Alice", "id": "csfd654321"}

        with patch.object(self.module, "get_sk_response", return_value=detail_response), patch.object(
            self.module, "extract_csfd_url_from_sk_detail", return_value="https://www.csfd.cz/film/654321"
        ) as mock_extract, patch.object(
            self.module, "get_normalized_csfd_data", return_value=csfd_data
        ) as mock_get_csfd_data, patch.object(
            self.module, "build_sk_output_filename", return_value="Movie Title#CZ#{Alice}(6 GB)[csfd654321].sk"
        ) as mock_build_name, patch.object(
            self.module,
            "write_list_to_file",
            return_value=True,
        ) as mock_write:
            self.module.visit_sk_url(result_item)

        mock_extract.assert_called_once_with(detail_response.text)
        mock_get_csfd_data.assert_called_once_with("https://www.csfd.cz/film/654321")
        mock_build_name.assert_called_once_with(result_item, csfd_data)
        mock_write.assert_called_once_with(
            str(Path(self.module.OUTPUT_DIR) / "Movie Title#CZ#{Alice}(6 GB)[csfd654321].sk"),
            ["https://example.com/torrent/details.php?name=movie&id=654"],
        )


class TestSkRedisHelpers(unittest.TestCase):
    """验证 SK 的 Redis 辅助逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_redis_client_uses_shared_config_connection(self):
        """应按共享 Redis 配置创建客户端。"""
        module, temp_dir = load_scrapy_sk(
            {
                "redis_host": "10.0.0.2",
                "redis_port": 6380,
                "redis_db": 5,
            }
        )
        self.addCleanup(temp_dir.cleanup)

        client = module.get_redis_client()

        self.assertEqual(client.kwargs["host"], "10.0.0.2")
        self.assertEqual(client.kwargs["port"], 6380)
        self.assertEqual(client.kwargs["db"], 5)
        self.assertTrue(client.kwargs["decode_responses"])

    def test_push_items_to_queue_deduplicates_by_url_for_sk_keys(self):
        """重复 URL 不应重复入队。"""
        items = [
            {
                "group": "2160p",
                "url": "https://example.com/torrent/details.php?name=movie&id=1",
                "title": "Movie A",
                "size": "10 GB",
                "date": "25/04/2026",
            },
            {
                "group": "2160p",
                "url": "https://example.com/torrent/details.php?name=movie&id=1",
                "title": "Movie A",
                "size": "10 GB",
                "date": "25/04/2026",
            },
        ]

        enqueued_count = self.module.push_items_to_queue(
            self.redis_client,
            items,
            seen_key=self.module.REDIS_SEEN_KEY,
            pending_key=self.module.REDIS_PENDING_KEY,
            unique_value=lambda item: item["url"],
            serializer=lambda item: self.module.serialize_payload(
                {
                    "group": item["group"],
                    "url": item["url"],
                    "title": item["title"],
                    "size": item["size"],
                    "date": item["date"],
                }
            ),
        )

        self.assertEqual(enqueued_count, 1)
        pending_payloads = self.redis_client.lrange(self.module.REDIS_PENDING_KEY, 0, -1)
        self.assertEqual(len(pending_payloads), 1)
        self.assertEqual(
            self.module._scrapy_redis.deserialize_payload(pending_payloads[0])["url"],
            "https://example.com/torrent/details.php?name=movie&id=1",
        )

    def test_recover_processing_queue_moves_items_back_to_pending_for_sk_keys(self):
        """中断残留在 processing 的任务应恢复回 pending。"""
        payload_a = self.module.serialize_payload(
            {"group": "A", "url": "u1", "title": "A", "size": "1 GB", "date": "25/04/2026"}
        )
        payload_b = self.module.serialize_payload(
            {"group": "B", "url": "u2", "title": "B", "size": "2 GB", "date": "24/04/2026"}
        )
        self.redis_client.rpush(self.module.REDIS_PENDING_KEY, payload_a)
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, payload_b)

        recovered_count = self.module._scrapy_redis.recover_processing_queue(
            self.redis_client,
            processing_key=self.module.REDIS_PROCESSING_KEY,
            pending_key=self.module.REDIS_PENDING_KEY,
            logger=self.module.logger,
            queue_label="SK",
        )

        self.assertEqual(recovered_count, 1)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PROCESSING_KEY), 0)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 2)

    def test_finalize_sk_run_updates_end_data_and_clears_run_state(self):
        """扫描完成且队列清空后，应回写 end_data 并清理本轮临时状态。"""
        failed_payload = self.module.serialize_payload(
            {"group": "A", "url": "u1", "title": "A", "size": "1 GB", "date": "25/04/2026"}
        )
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")
        self.redis_client.set(self.module.REDIS_NEXT_END_DATA_KEY, "24/04/2026")
        self.redis_client.set(self.module.REDIS_SCAN_PAGE_KEY, "3")
        self.redis_client.sadd(self.module.REDIS_SEEN_KEY, "u1")
        self.redis_client.rpush(self.module.REDIS_FAILED_KEY, failed_payload)

        with patch.object(self.module, "update_json_config") as mock_update:
            self.module.finalize_sk_run(redis_client=self.redis_client)

        mock_update.assert_called_once_with("config/scrapy_sk.json", "end_data", "24/04/2026")
        self.assertIsNone(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY))
        self.assertIsNone(self.redis_client.get(self.module.REDIS_NEXT_END_DATA_KEY))
        self.assertEqual(self.redis_client.llen(self.module.REDIS_FAILED_KEY), 1)

    def test_finalize_sk_run_skips_update_when_pending_tasks_remain(self):
        """仍有未完成任务时，不应提前回写 end_data。"""
        payload = self.module.serialize_payload(
            {"group": "A", "url": "u1", "title": "A", "size": "1 GB", "date": "25/04/2026"}
        )
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")
        self.redis_client.set(self.module.REDIS_NEXT_END_DATA_KEY, "24/04/2026")
        self.redis_client.rpush(self.module.REDIS_PENDING_KEY, payload)

        with patch.object(self.module, "update_json_config") as mock_update:
            self.module.finalize_sk_run(redis_client=self.redis_client)

        mock_update.assert_not_called()

    def test_finalize_sk_run_skips_update_when_scan_is_incomplete(self):
        """列表扫描未完成时，不应提前回写 end_data。"""
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "0")
        self.redis_client.set(self.module.REDIS_NEXT_END_DATA_KEY, "24/04/2026")

        with patch.object(self.module, "update_json_config") as mock_update:
            self.module.finalize_sk_run(redis_client=self.redis_client)

        mock_update.assert_not_called()
        self.assertEqual(self.redis_client.get(self.module.REDIS_NEXT_END_DATA_KEY), "24/04/2026")

    def test_finalize_sk_run_skips_update_when_next_end_data_is_missing(self):
        """缺少新的截止日期时，不应回写配置或清理本轮状态。"""
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")
        self.redis_client.set(self.module.REDIS_SCAN_PAGE_KEY, "5")

        with patch.object(self.module, "update_json_config") as mock_update:
            self.module.finalize_sk_run(redis_client=self.redis_client)

        mock_update.assert_not_called()
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY), "1")
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_PAGE_KEY), "5")


class TestFetchSkPage(unittest.TestCase):
    """验证单页抓取流程。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_fetch_sk_page_returns_parsed_result_list(self):
        """应请求列表页并返回解析结果。"""
        parsed_page = [{"date": "15/10/2013"}]

        with patch.object(self.module, "get_sk_response", return_value=Mock()) as mock_get, patch.object(
            self.module, "parse_sk_response", return_value=parsed_page
        ):
            result = self.module.fetch_sk_page(0)

        self.assertEqual(result, parsed_page)
        mock_get.assert_called_once_with("https://example.com/browse?page=0")

    def test_fetch_sk_page_raises_when_parsed_page_is_empty(self):
        """整页解析结果为空时，应显式报错停止后续翻页。"""
        with patch.object(self.module, "get_sk_response", return_value=Mock(text="<html></html>")), patch.object(
            self.module, "parse_sk_response", return_value=[]
        ):
            with self.assertRaisesRegex(RuntimeError, "解析结果为空"):
                self.module.fetch_sk_page(1)

    def test_fetch_sk_page_allows_known_filtered_empty_page(self):
        """账户过滤导致的已知空页应返回空列表并继续交给上层处理。"""
        response = Mock(text=build_sk_filtered_empty_page_html())

        with patch.object(self.module, "get_sk_response", return_value=response), patch.object(
            self.module, "parse_sk_response", return_value=[]
        ):
            result = self.module.fetch_sk_page(17)

        self.assertEqual(result, [])

    def test_fetch_sk_page_allows_page_with_only_excluded_groups(self):
        """整页只有排除分组帖子时，应返回空列表供上层继续翻页。"""
        response = Mock(
            text=build_sk_page_html(
                build_sk_item_html(
                    group="Knihy a Časopisy",
                    group_href="torrents_v2.php?category=23",
                    title="Computer (04/2026) (CZ)",
                    detail_href="details.php?name=computer&id=999",
                    metadata="Velkost 79.3 MB | Pridany 25/03/2026",
                )
            )
        )

        with patch.object(self.module, "get_sk_response", return_value=response):
            result = self.module.fetch_sk_page(25)

        self.assertEqual(result, [])


class TestEnqueueSkPosts(unittest.TestCase):
    """验证 SK 列表扫描入队逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_enqueue_sk_posts_scans_until_end_date_and_persists_resume_state(self):
        """应逐页入队，命中截止日期后标记扫描完成。"""
        parsed_pages = [
            [
                {"group": "A", "url": "u1", "title": "Movie A", "size": "1 GB", "date": "25/04/2026"},
                {"group": "B", "url": "u2", "title": "Movie B", "size": "2 GB", "date": "24/04/2026"},
            ],
            [
                {"group": "C", "url": "u3", "title": "Movie C", "size": "3 GB", "date": "15/10/2013"},
            ],
        ]

        with patch.object(self.module, "fetch_sk_page", side_effect=parsed_pages) as mock_fetch:
            self.module.enqueue_sk_posts(start_page=0, end_data="15/10/2013", redis_client=self.redis_client)

        self.assertEqual(mock_fetch.call_args_list, [call(0), call(1)])
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY), "1")
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_PAGE_KEY), "2")
        self.assertEqual(self.redis_client.get(self.module.REDIS_NEXT_END_DATA_KEY), "24/04/2026")
        pending_payloads = self.redis_client.lrange(self.module.REDIS_PENDING_KEY, 0, -1)
        pending_urls = [self.module._scrapy_redis.deserialize_payload(payload)["url"] for payload in pending_payloads]
        self.assertEqual(pending_urls, ["u1", "u2", "u3"])

    def test_enqueue_sk_posts_resumes_from_saved_page_and_keeps_existing_next_end_data(self):
        """恢复扫描时应从 Redis 里的页码继续，且不重算已有的下一轮截止日期。"""
        self.redis_client.set(self.module.REDIS_SCAN_PAGE_KEY, "3")
        self.redis_client.set(self.module.REDIS_NEXT_END_DATA_KEY, "24/03/2026")

        with patch.object(
            self.module,
            "fetch_sk_page",
            return_value=[{"group": "A", "url": "u4", "title": "Movie D", "size": "4 GB", "date": "01/05/2026"}],
        ) as mock_fetch:
            self.module.enqueue_sk_posts(start_page=0, end_data="01/05/2026", redis_client=self.redis_client)

        mock_fetch.assert_called_once_with(3)
        self.assertEqual(self.redis_client.get(self.module.REDIS_NEXT_END_DATA_KEY), "24/03/2026")
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY), "1")

    def test_enqueue_sk_posts_returns_immediately_when_scan_is_already_complete(self):
        """Redis 中已标记扫描完成时，不应继续请求列表页。"""
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")

        with patch.object(self.module, "fetch_sk_page") as mock_fetch:
            self.module.enqueue_sk_posts(start_page=0, redis_client=self.redis_client)

        mock_fetch.assert_not_called()

    def test_enqueue_sk_posts_skips_filtered_empty_pages_until_results_resume(self):
        """连续空页未达到上限时，应继续翻页直到抓到有效帖子。"""
        with patch.object(
            self.module,
            "fetch_sk_page",
            side_effect=[
                [],
                [],
                [{"group": "A", "url": "u9", "title": "Movie I", "size": "9 GB", "date": "15/10/2013"}],
            ],
        ) as mock_fetch:
            self.module.enqueue_sk_posts(start_page=17, end_data="15/10/2013", redis_client=self.redis_client)

        self.assertEqual(mock_fetch.call_args_list, [call(17), call(18), call(19)])
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY), "1")
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_PAGE_KEY), "20")
        self.assertEqual(self.redis_client.get(self.module.REDIS_NEXT_END_DATA_KEY), "14/10/2013")
        pending_payloads = self.redis_client.lrange(self.module.REDIS_PENDING_KEY, 0, -1)
        pending_urls = [self.module._scrapy_redis.deserialize_payload(payload)["url"] for payload in pending_payloads]
        self.assertEqual(pending_urls, ["u9"])

    def test_enqueue_sk_posts_raises_after_too_many_empty_pages(self):
        """连续空页超过上限时，应停止扫描避免无限翻页。"""
        module, temp_dir = load_scrapy_sk({"max_empty_pages": 3})
        self.addCleanup(temp_dir.cleanup)
        redis_client = FakeRedis()

        with patch.object(module, "fetch_sk_page", side_effect=[[], [], []]) as mock_fetch:
            with self.assertRaisesRegex(RuntimeError, "连续 3 页无有效帖子"):
                module.enqueue_sk_posts(start_page=17, redis_client=redis_client)

        self.assertEqual(mock_fetch.call_args_list, [call(17), call(18), call(19)])


class TestDrainSkQueue(unittest.TestCase):
    """验证 SK 队列消费逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_drain_sk_queue_processes_pending_items_and_records_failures(self):
        """消费队列时，成功任务应清理 processing，失败任务应进入 failed。"""
        payload_success = self.module.serialize_payload(
            {"group": "A", "url": "https://example.com/topic/101", "title": "A", "size": "1 GB", "date": "25/04/2026"}
        )
        payload_fail = self.module.serialize_payload(
            {"group": "B", "url": "https://example.com/topic/102", "title": "B", "size": "2 GB", "date": "24/04/2026"}
        )
        self.redis_client.rpush(self.module.REDIS_PENDING_KEY, payload_success)
        self.redis_client.rpush(self.module.REDIS_PENDING_KEY, payload_fail)

        def fake_visit_sk_url(info: dict) -> None:
            if info["url"].endswith("102"):
                raise RuntimeError("boom")

        with patch.object(self.module, "visit_sk_url", side_effect=fake_visit_sk_url) as mock_visit, patch.object(
            self.module.logger, "error"
        ) as mock_error:
            self.module.drain_sk_queue(redis_client=self.redis_client)

        self.assertEqual(mock_visit.call_count, 2)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 0)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PROCESSING_KEY), 0)
        failed_payloads = self.redis_client.lrange(self.module.REDIS_FAILED_KEY, 0, -1)
        self.assertEqual(len(failed_payloads), 1)
        self.assertEqual(
            self.module._scrapy_redis.deserialize_payload(failed_payloads[0])["url"],
            "https://example.com/topic/102",
        )
        self.assertIn("https://example.com/topic/102", mock_error.call_args[0][0])

    def test_drain_sk_queue_logs_when_queue_is_empty(self):
        """没有待处理任务时，应输出空队列提示并直接返回。"""
        with patch.object(self.module.logger, "info") as mock_info:
            self.module.drain_sk_queue(redis_client=self.redis_client)

        self.assertIn("SK 队列为空，没有待处理任务", [call.args[0] for call in mock_info.call_args_list])


class TestScrapySkMain(unittest.TestCase):
    """验证主抓取流程的编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_sk_calls_enqueue_drain_and_finalize_with_shared_redis_client(self):
        """入口函数应依次执行入队、消费和收尾，并复用同一个 Redis 客户端。"""
        with patch.object(self.module, "get_redis_client", return_value=self.redis_client) as mock_get_redis, patch.object(
            self.module, "enqueue_sk_posts"
        ) as mock_enqueue, patch.object(
            self.module, "drain_sk_queue"
        ) as mock_drain, patch.object(
            self.module, "finalize_sk_run"
        ) as mock_finalize:
            self.module.scrapy_sk(start_page=2)

        mock_get_redis.assert_called_once_with()
        mock_enqueue.assert_called_once_with(start_page=2, redis_client=self.redis_client)
        mock_drain.assert_called_once_with(redis_client=self.redis_client)
        mock_finalize.assert_called_once_with(redis_client=self.redis_client)

    def test_scrapy_sk_stops_when_enqueue_sk_posts_raises(self):
        """入队阶段报错时，不应继续消费队列或执行收尾。"""
        with patch.object(self.module, "get_redis_client", return_value=self.redis_client), patch.object(
            self.module, "enqueue_sk_posts", side_effect=RuntimeError("enqueue boom")
        ) as mock_enqueue, patch.object(
            self.module, "drain_sk_queue"
        ) as mock_drain, patch.object(
            self.module, "finalize_sk_run"
        ) as mock_finalize:
            with self.assertRaisesRegex(RuntimeError, "enqueue boom"):
                self.module.scrapy_sk(start_page=1)

        mock_enqueue.assert_called_once_with(start_page=1, redis_client=self.redis_client)
        mock_drain.assert_not_called()
        mock_finalize.assert_not_called()

    def test_scrapy_sk_does_not_finalize_when_drain_sk_queue_raises(self):
        """消费阶段报错时，应向上抛出且不执行收尾。"""
        with patch.object(self.module, "get_redis_client", return_value=self.redis_client), patch.object(
            self.module, "enqueue_sk_posts"
        ) as mock_enqueue, patch.object(
            self.module, "drain_sk_queue", side_effect=RuntimeError("drain boom")
        ) as mock_drain, patch.object(
            self.module, "finalize_sk_run"
        ) as mock_finalize:
            with self.assertRaisesRegex(RuntimeError, "drain boom"):
                self.module.scrapy_sk(start_page=3)

        mock_enqueue.assert_called_once_with(start_page=3, redis_client=self.redis_client)
        mock_drain.assert_called_once_with(redis_client=self.redis_client)
        mock_finalize.assert_not_called()


class TestScrapySkDateHelpers(unittest.TestCase):
    """验证 SK 截止日期 helper。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_sk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_previous_day_returns_previous_calendar_day(self):
        """应返回输入日期的前一天。"""
        self.assertEqual(self.module.get_previous_day("01/05/2026"), "30/04/2026")

    def test_get_current_end_data_reads_latest_value_from_config(self):
        """应在运行时读取当前配置里的截止日期。"""
        with patch.object(self.module, "read_json_to_dict", return_value={"end_data": "01/05/2026"}):
            self.assertEqual(self.module.get_current_end_data(), "01/05/2026")


if __name__ == "__main__":
    unittest.main()
