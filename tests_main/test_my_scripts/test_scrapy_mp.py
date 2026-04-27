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
try:
    import fakeredis
except ImportError:  # pragma: no cover - 由依赖安装状态决定
    fakeredis = None

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_mp.py"
REDIS_HELPER_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_redis.py"


def load_scrapy_mp(config: dict | None = None):
    """在最小依赖环境中加载 ``scrapy_mp`` 模块。"""
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "mp_url": "https://example.com",
        "mp_movie_url": "https://example.com/movies/page/",
        "mp_cookie": "cookie=value",
        "request_head": {"User-Agent": "unit-test"},
        "output_dir": temp_dir.name,
        "thread_number": 3,
        "redis_pending_key": "mp_pending",
        "redis_processing_key": "mp_processing",
        "redis_seen_key": "mp_seen",
        "redis_scan_page_key": "mp_scan_page",
        "redis_scan_complete_key": "mp_scan_complete",
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

    fake_redis = types.ModuleType("redis")

    class DummyRedis:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    fake_redis.Redis = DummyRedis

    helper_spec = importlib.util.spec_from_file_location(
        f"scrapy_redis_test_{uuid.uuid4().hex}",
        REDIS_HELPER_PATH,
    )
    helper_module = importlib.util.module_from_spec(helper_spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "redis": fake_redis,
        },
    ):
        helper_spec.loader.exec_module(helper_module)

    spec = importlib.util.spec_from_file_location(
        f"scrapy_mp_test_{uuid.uuid4().hex}",
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

    def test_request_mp_page_error_prefers_explicit_verification_url(self):
        """命中 CF 时，错误提示应优先使用配置里的验证页。"""
        module, temp_dir = load_scrapy_mp(
            {
                "mp_verification_url": "https://movieparadise.org/movies/the-ikon-of-elijah/",
            }
        )
        self.addCleanup(temp_dir.cleanup)

        response = Mock(
            status_code=403,
            text='<!DOCTYPE html><html><head><title>Just a moment...</title></head><body>cf_chl</body></html>',
        )
        with patch.object(module.requests, "get", return_value=response):
            with self.assertRaisesRegex(module.MpCloudflareError, "the-ikon-of-elijah"):
                module.request_mp_page("https://example.com/post")


class TestGetMpResponse(unittest.TestCase):
    """验证单次请求逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_mp_response_returns_response_and_sets_utf8_encoding(self):
        """请求成功时应返回响应对象，并统一设置 UTF-8 编码。"""
        response = Mock(status_code=200)
        expected_headers = dict(self.module.REQUEST_HEAD)
        expected_headers["Cookie"] = "cookie=value"

        with patch.object(self.module.requests, "get", return_value=response) as mock_get:
            result = self.module.get_mp_response("https://example.com/post")

        self.assertIs(result, response)
        self.assertEqual(response.encoding, "utf-8")
        mock_get.assert_called_once_with("https://example.com/post", headers=expected_headers, timeout=20)

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

    def test_get_mp_response_raises_cookie_error_on_cloudflare_challenge_page(self):
        """命中 Cloudflare 验证页时应直接报 Cookie/验证失效。"""
        response = Mock(
            status_code=403,
            text='<!DOCTYPE html><html><head><title>Just a moment...</title></head><body>https://challenges.cloudflare.com</body></html>',
        )
        expected_headers = dict(self.module.REQUEST_HEAD)
        expected_headers["Cookie"] = "cookie=value"

        with patch.object(self.module.requests, "get", return_value=response) as mock_get:
            with self.assertRaisesRegex(self.module.MpCloudflareError, "Cloudflare"):
                self.module.get_mp_response("https://example.com/post")
        mock_get.assert_called_once_with("https://example.com/post", headers=expected_headers, timeout=20)

    def test_get_mp_response_stops_immediately_on_cloudflare(self):
        """命中 CF 时不应再尝试自动刷新 Cookie。"""
        cf_response = Mock(
            status_code=403,
            text='<!DOCTYPE html><html><head><title>Just a moment...</title></head><body>cf_chl</body></html>',
        )

        with patch.object(self.module.requests, "get", return_value=cf_response):
            with self.assertRaisesRegex(self.module.MpCloudflareError, "Cloudflare"):
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

    def test_parse_mp_response_delegates_each_article_to_helper(self):
        """整页解析应逐条委托给 ``parse_mp_article``。"""
        response = Mock(
            text=build_archive_page(
                build_archive_article(title="Movie A", link="https://example.com/a", span_text="1990"),
                build_archive_article(title="Movie B", link="https://example.com/b", span_text="1991"),
            )
        )

        with patch.object(
            self.module,
            "parse_mp_article",
            side_effect=[
                {"title": "Movie A", "link": "https://example.com/a", "year": "1990"},
                None,
            ],
        ) as mock_parse_article:
            result = self.module.parse_mp_response(response)

        self.assertEqual(
            result,
            [{"title": "Movie A", "link": "https://example.com/a", "year": "1990"}],
        )
        self.assertEqual(mock_parse_article.call_count, 2)


class TestParseMpArticle(unittest.TestCase):
    """验证单个列表页条目解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_mp_article_extracts_title_link_and_year(self):
        """应从单条 ``article`` 里提取标题、链接和年份。"""
        response = Mock(
            text=build_archive_page(
                build_archive_article(
                    title="Movie Title",
                    link="https://example.com/post-1",
                    span_text="Jul. 20, 1990",
                )
            )
        )
        article = self.module.BeautifulSoup(response.text, "html.parser").find("article")

        result = self.module.parse_mp_article(article)

        self.assertEqual(
            result,
            {
                "title": "Movie Title",
                "link": "https://example.com/post-1",
                "year": "1990",
            },
        )

    def test_parse_mp_article_returns_none_when_h3_anchor_is_missing(self):
        """条目缺少标题链接时应返回 ``None``。"""
        article = self.module.BeautifulSoup(
            '<article class="item movies"><div class="data"><span>1990</span></div></article>',
            "html.parser",
        ).find("article")

        with self.assertLogs(self.module.logger.name, level="WARNING") as logs:
            result = self.module.parse_mp_article(article)

        self.assertIsNone(result)
        self.assertIn("mp 列表条目缺少 h3 标题节点，已跳过", logs.output[0])

    def test_parse_mp_article_keeps_empty_year_without_warning(self):
        """条目缺少年份时仍保留标题和链接，年份留空。"""
        article = self.module.BeautifulSoup(
            build_archive_article(
                title="Movie Title",
                link="https://example.com/post-1",
                span_text="Unknown date",
            ),
            "html.parser",
        ).find("article")

        result = self.module.parse_mp_article(article)

        self.assertEqual(
            result,
            {
                "title": "Movie Title",
                "link": "https://example.com/post-1",
                "year": "",
            },
        )


class TestMpQueueHelpers(unittest.TestCase):
    """验证 MP 入队阶段使用的辅助函数。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_normalize_mp_end_urls_supports_string_and_iterable_inputs(self):
        """截止条件应统一整理成去空白、去空值的 URL 集合。"""
        self.assertEqual(
            self.module.normalize_mp_end_urls(" https://example.com/a "),
            {"https://example.com/a"},
        )
        self.assertEqual(
            self.module.normalize_mp_end_urls(["https://example.com/a", "", " https://example.com/b "]),
            {"https://example.com/a", "https://example.com/b"},
        )

    def test_get_mp_queued_links_reads_pending_and_processing_urls(self):
        """应从 Redis 当前两条队列里提取全部已入队 URL。"""
        redis_client = FakeRedis()
        redis_client.rpush(
            self.module.REDIS_PENDING_KEY,
            self.module._scrapy_redis.serialize_payload(
                {"link": "https://example.com/pending", "title": "Pending", "year": "2026"}
            ),
        )
        redis_client.rpush(
            self.module.REDIS_PROCESSING_KEY,
            self.module._scrapy_redis.serialize_payload(
                {"link": "https://example.com/processing", "title": "Processing", "year": "2026"}
            ),
        )

        self.assertEqual(
            self.module.get_mp_queued_links(redis_client),
            {"https://example.com/pending", "https://example.com/processing"},
        )

    def test_prepare_mp_enqueue_scan_returns_saved_page_and_matched_end_urls(self):
        """准备阶段应恢复页码，并累计当前队列里已命中的截止 URL。"""
        redis_client = FakeRedis()
        redis_client.set(self.module.REDIS_SCAN_PAGE_KEY, "5")
        redis_client.rpush(
            self.module.REDIS_PENDING_KEY,
            self.module._scrapy_redis.serialize_payload(
                {"link": "https://example.com/end-a", "title": "Queued", "year": "2026"}
            ),
        )

        with patch.object(self.module, "validate_mp_end_urls") as mock_validate:
            current_page, end_urls, queued_links, matched_end_urls = self.module.prepare_mp_enqueue_scan(
                start_page=2,
                end=["https://example.com/end-a", "https://example.com/end-b"],
                redis_client=redis_client,
            )

        self.assertEqual(current_page, 5)
        self.assertEqual(end_urls, {"https://example.com/end-a", "https://example.com/end-b"})
        self.assertEqual(queued_links, {"https://example.com/end-a"})
        self.assertEqual(matched_end_urls, {"https://example.com/end-a"})
        mock_validate.assert_called_once_with({"https://example.com/end-a", "https://example.com/end-b"})

    def test_split_new_mp_items_filters_empty_and_duplicate_links(self):
        """单页分类应排除空链接、当前队列重复和页内重复。"""
        result_list = [
            {"title": "Movie 1", "link": "https://example.com/existing", "year": "2026"},
            {"title": "Movie 2", "link": "https://example.com/new", "year": "2026"},
            {"title": "Movie 3", "link": "https://example.com/new", "year": "2026"},
            {"title": "Movie 4", "link": "", "year": "2026"},
        ]

        new_items, page_unique_links = self.module.split_new_mp_items(
            result_list,
            {"https://example.com/existing"},
        )

        self.assertEqual(
            new_items,
            [{"title": "Movie 2", "link": "https://example.com/new", "year": "2026"}],
        )
        self.assertEqual(page_unique_links, {"https://example.com/new"})

    def test_validate_mp_end_urls_raises_when_any_url_returns_404(self):
        """截止 URL 若已 404，应在运行前直接报错。"""
        responses = [
            Mock(status_code=200),
            Mock(status_code=404),
        ]

        with patch.object(self.module.requests, "get", side_effect=responses):
            with self.assertRaisesRegex(ValueError, "404"):
                self.module.validate_mp_end_urls(
                    {"https://example.com/end-a", "https://example.com/end-b"}
                )

    def test_validate_mp_end_urls_raises_cookie_error_on_cloudflare_challenge(self):
        """截止 URL 校验阶段若命中 CF 验证页，也应直接终止。"""
        response = Mock(
            status_code=403,
            text='<!DOCTYPE html><html><head><title>Just a moment...</title></head><body>cf_chl</body></html>',
        )

        with patch.object(self.module.requests, "get", return_value=response):
            with self.assertRaisesRegex(self.module.MpCloudflareError, "Cloudflare"):
                self.module.validate_mp_end_urls({"https://example.com/end-a"})

class TestFormatMpText(unittest.TestCase):
    """验证正文格式化逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_format_mp_text_inserts_two_blank_lines_before_each_release_line(self):
        """每个 ``Release:`` 行前都应插入两空行，便于阅读。"""
        text = "\n".join(
            [
                "Other versions available:",
                "Release: 180 2026 1080p NF WEB-DL",
                "General: mkv | 3.63 GB",
                "Rapidgator #1",
                "Release: 180 2026 720p NF WEB-DL",
            ]
        )

        result = self.module.format_mp_text(text)

        self.assertEqual(
            result,
            "\n".join(
                [
                    "Other versions available:",
                    "",
                    "",
                    "Release: 180 2026 1080p NF WEB-DL ~ 3.63 GB",
                    "General: mkv | 3.63 GB",
                    "Rapidgator #1",
                    "",
                    "",
                    "Release: 180 2026 720p NF WEB-DL",
                ]
            ),
        )

    def test_format_mp_text_removes_screenshot_lines_and_keeps_download_links(self):
        """应只清洗截图段里的图片行，并保留后续下载链接。"""
        text = "\n".join(
            [
                "Screenshots:",
                "#1 (https://img2.pixhost.to/images/7318/sample.png)",
                "https://image.tmdb.org/t/p/original/in-section.jpg",
                "Rapidgator #1 ~ 3.63 GB (https://rapidgator.net/file/sample.rar)",
                "SCREENSHOTS (https://i.postimg.cc/ZnpckZLj/sample.jpg)",
                "Rapidgator",
                "file.rar (https://rapidgator.net/file/next.rar)",
            ]
        )

        result = self.module.format_mp_text(text)

        self.assertEqual(
            result,
            "\n".join(
                [
                    "Rapidgator #1 ~ 3.63 GB (https://rapidgator.net/file/sample.rar)",
                    "Rapidgator",
                    "file.rar (https://rapidgator.net/file/next.rar)",
                ]
            ),
        )

    def test_format_mp_text_removes_tmdb_image_url_outside_screenshot_section(self):
        """截图段之外的 TMDb 图片地址也应删除。"""
        text = "\n".join(
            [
                "Rapidgator #1 ~ 3.63 GB (https://rapidgator.net/file/sample.rar)",
                "https://image.tmdb.org/t/p/original/standalone.jpg",
            ]
        )

        result = self.module.format_mp_text(text)

        self.assertEqual(
            result,
            "Rapidgator #1 ~ 3.63 GB (https://rapidgator.net/file/sample.rar)",
        )

    def test_format_mp_text_keeps_non_tmdb_image_url_outside_screenshot_section(self):
        """非 TMDb 的游离图片地址仍先保留，避免规则继续扩大。"""
        text = "\n".join(
            [
                "Rapidgator #1 ~ 3.63 GB (https://rapidgator.net/file/sample.rar)",
                "https://example.com/poster.jpg",
            ]
        )

        result = self.module.format_mp_text(text)

        self.assertEqual(
            result,
            "\n".join(
                [
                    "Rapidgator #1 ~ 3.63 GB (https://rapidgator.net/file/sample.rar)",
                    "https://example.com/poster.jpg",
                ]
            ),
        )


class TestExtractMpSizeCandidate(unittest.TestCase):
    """验证单行大小提取逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_extract_mp_size_candidate_supports_general_length_size_and_file_size(self):
        """应支持旧样本中最常见的几类大小来源行。"""
        self.assertEqual(
            self.module.extract_mp_size_candidate("General: mp4 | 1285 Kbps | 754 MB | 01:21:55"),
            ("general", "754 MB"),
        )
        self.assertEqual(
            self.module.extract_mp_size_candidate("Length           : 1.31 GiB for 01:48:36"),
            ("length", "1.31 GiB"),
        )
        self.assertEqual(
            self.module.extract_mp_size_candidate("Size: 1179357777 bytes (1.10 GiB), duration: 01:30:52"),
            ("size", "1.10 GiB"),
        )
        self.assertEqual(
            self.module.extract_mp_size_candidate("File size           : 949 MiB"),
            ("file size", "949 MiB"),
        )

    def test_extract_mp_size_candidate_uses_rapidgator_as_last_resort(self):
        """带大小的 Rapidgator 行应作为兜底候选返回。"""
        self.assertEqual(
            self.module.extract_mp_size_candidate(
                "Rapidgator #1 ~ 5 GB (https://rapidgator.net/file/sample.part1.rar)"
            ),
            ("rapidgator", "5 GB"),
        )


class TestFillMpReleaseSizes(unittest.TestCase):
    """验证 Release 行大小补全逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_fill_mp_release_sizes_fills_from_general_line(self):
        """当前新样本里缺大小的 Release 行应优先从 ``General`` 行补齐。"""
        lines = [
            "Release: Bjornoya 2014 NORWEGIAN 720p BluRay H264 AAC-VXT",
            "General: mp4 | 1 GB | 01:22:44",
            "Rapidgator",
        ]

        result = self.module.fill_mp_release_sizes(lines)

        self.assertEqual(
            result[0],
            "Release: Bjornoya 2014 NORWEGIAN 720p BluRay H264 AAC-VXT ~ 1 GB",
        )

    def test_fill_mp_release_sizes_fills_from_length_when_general_is_missing(self):
        """没有 ``General`` 时应回退到 ``Length`` 行。"""
        lines = [
            "Release: Bound 1996 720p BRRip H264 AAC-RARBG",
            "Length           : 1.31 GiB for 01:48:36",
            "Rapidgator",
        ]

        result = self.module.fill_mp_release_sizes(lines)

        self.assertEqual(
            result[0],
            "Release: Bound 1996 720p BRRip H264 AAC-RARBG ~ 1.31 GiB",
        )

    def test_fill_mp_release_sizes_uses_rapidgator_only_as_fallback(self):
        """当技术信息里没有大小时，才回退到带大小的 ``Rapidgator`` 行。"""
        lines = [
            "Release: Buddhas Palm 1982 1080p BluRay x264-SHAOLiN",
            "General: mkv | 175 B | 00:00:56",
            "Rapidgator #1 ~ 5 GB (https://rapidgator.net/file/sample.part1.rar)",
        ]

        result = self.module.fill_mp_release_sizes(lines)

        self.assertEqual(
            result[0],
            "Release: Buddhas Palm 1982 1080p BluRay x264-SHAOLiN ~ 5 GB",
        )


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
                "file_name": "Safe Title(1990) - mpvd [tt1234567].rare",
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
                "file_name": "Movie Title(1990) - mpvd [tmdb98765].rare",
                "content": "Plain text",
            },
        )

    def test_parse_mp_detail_raises_when_custom_fields_are_missing(self):
        """缺少编号区块时应抛异常，避免任务被静默吞掉。"""
        response = Mock(text='<html><body><div itemprop="description" class="wp-content"><p>Body</p></div></body></html>')

        with self.assertRaisesRegex(ValueError, "custom_fields2"):
            self.module.parse_mp_detail(
                response,
                {
                    "title": "Movie Title",
                    "link": "https://example.com/post",
                    "year": "1990",
                },
            )

    def test_parse_mp_detail_raises_when_description_is_missing(self):
        """缺少正文区块时应抛异常，留待下次重跑。"""
        response = Mock(text=build_detail_page(id_links=["https://www.imdb.com/title/tt1234567/"], description_html=None))

        with self.assertRaisesRegex(ValueError, "description"):
            self.module.parse_mp_detail(
                response,
                {
                    "title": "Movie Title",
                    "link": "https://example.com/post",
                    "year": "1990",
                },
            )

    def test_parse_mp_detail_formats_release_sections_for_readability(self):
        """正文里的 ``Release:`` 段落前应补两空行。"""
        response = Mock(
            text=build_detail_page(
                id_links=["https://www.imdb.com/title/tt1234567/"],
                description_html=(
                    "<p>Other versions available:</p>"
                    "<p>Release: 180 2026 1080p NF WEB-DL</p>"
                    "<p>General: mkv | 3.63 GB</p>"
                    "<p>Screenshots:</p>"
                    '<p><a href="https://img2.pixhost.to/images/7318/sample.png">#1</a></p>'
                    '<p><a href="https://rapidgator.net/file/sample.rar">Rapidgator #1 ~ 3.63 GB</a></p>'
                ),
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
            result["content"],
            "Other versions available:\n\n\nRelease: 180 2026 1080p NF WEB-DL ~ 3.63 GB\nGeneral: mkv | 3.63 GB\nRapidgator #1 ~ 3.63 GB (https://rapidgator.net/file/sample.rar)",
        )


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

    def test_visit_mp_url_raises_when_detail_result_is_not_dict(self):
        """解析返回非法类型时应抛异常，避免任务被当成成功。"""
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
            with self.assertRaisesRegex(TypeError, "无效"):
                self.module.visit_mp_url(result_item)

        mock_parse.assert_called_once_with(response, result_item)
        mock_write.assert_not_called()


class TestMpRedisFlow(unittest.TestCase):
    """验证 MP 的 Redis 入队和消费流程。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_enqueue_mp_posts_scans_until_all_end_urls_are_matched_across_pages(self):
        """应跨页累计命中全部截止 URL 后再停止翻页。"""
        with patch.object(self.module, "validate_mp_end_urls") as mock_validate, patch.object(
            self.module,
            "get_mp_response",
            side_effect=[Mock(), Mock()],
        ) as mock_get, patch.object(
            self.module,
            "parse_mp_response",
            side_effect=[
                [
                    {"title": "Movie 1", "link": "https://example.com/new-1", "year": "2026"},
                    {"title": "Movie 2", "link": "https://example.com/end-a", "year": "2026"},
                ],
                [
                    {"title": "Movie 3", "link": "https://example.com/new-3", "year": "2026"},
                    {"title": "Movie old", "link": "https://example.com/end-b", "year": "2025"},
                    {"title": "Movie older", "link": "https://example.com/older", "year": "2025"},
                ],
            ],
        ):
            self.module.enqueue_mp_posts(
                start_page=2,
                end=["https://example.com/end-a", "https://example.com/end-b"],
                redis_client=self.redis_client,
            )

        mock_validate.assert_called_once_with(
            {"https://example.com/end-a", "https://example.com/end-b"}
        )
        self.assertEqual(
            mock_get.call_args_list,
            [
                call("https://example.com/movies/page/2/"),
                call("https://example.com/movies/page/3/"),
            ],
        )
        payloads = [
            self.module._scrapy_redis.deserialize_payload(payload)
            for payload in self.redis_client.lrange(self.module.REDIS_PENDING_KEY, 0, -1)
        ]
        self.assertEqual(
            payloads,
            [
                {"link": "https://example.com/new-1", "title": "Movie 1", "year": "2026"},
                {"link": "https://example.com/end-a", "title": "Movie 2", "year": "2026"},
                {"link": "https://example.com/new-3", "title": "Movie 3", "year": "2026"},
                {"link": "https://example.com/end-b", "title": "Movie old", "year": "2025"},
                {"link": "https://example.com/older", "title": "Movie older", "year": "2025"},
            ],
        )

    def test_enqueue_mp_posts_counts_end_urls_already_in_active_queue(self):
        """活跃队列里已有的截止 URL 也应参与累计命中判断。"""
        self.redis_client.rpush(
            self.module.REDIS_PENDING_KEY,
            self.module._scrapy_redis.serialize_payload(
                {"link": "https://example.com/end-a", "title": "Queued", "year": "2026"}
            ),
        )

        with patch.object(self.module, "validate_mp_end_urls") as mock_validate, patch.object(
            self.module, "get_mp_response", return_value=Mock()
        ) as mock_get, patch.object(
            self.module,
            "parse_mp_response",
            return_value=[
                {"title": "Movie 1", "link": "https://example.com/new-1", "year": "2026"},
                {"title": "Movie 2", "link": "https://example.com/end-b", "year": "2026"},
            ],
        ):
            self.module.enqueue_mp_posts(
                start_page=2,
                end=["https://example.com/end-a", "https://example.com/end-b"],
                redis_client=self.redis_client,
            )

        mock_validate.assert_called_once_with(
            {"https://example.com/end-a", "https://example.com/end-b"}
        )
        mock_get.assert_called_once_with("https://example.com/movies/page/2/")

    def test_enqueue_mp_posts_skips_links_already_present_in_current_queue(self):
        """当前未完成轮次里已排队的帖子，不应再次入队。"""
        self.redis_client.rpush(
            self.module.REDIS_PENDING_KEY,
            self.module._scrapy_redis.serialize_payload(
                {"link": "https://example.com/existing", "title": "Existing", "year": "2026"}
            ),
        )

        with patch.object(self.module, "validate_mp_end_urls"), patch.object(
            self.module,
            "get_mp_response",
            return_value=Mock(),
        ), patch.object(
            self.module,
            "parse_mp_response",
            return_value=[
                {"title": "Existing Again", "link": "https://example.com/existing", "year": "2026"},
                {"title": "Brand New", "link": "https://example.com/new", "year": "2026"},
            ],
        ):
            self.module.enqueue_mp_posts(
                start_page=2,
                end=["https://example.com/new"],
                redis_client=self.redis_client,
            )

        payloads = [
            self.module._scrapy_redis.deserialize_payload(payload)
            for payload in self.redis_client.lrange(self.module.REDIS_PENDING_KEY, 0, -1)
        ]
        self.assertEqual(
            payloads,
            [
                {"link": "https://example.com/existing", "title": "Existing", "year": "2026"},
                {"link": "https://example.com/new", "title": "Brand New", "year": "2026"},
            ],
        )

    def test_enqueue_mp_posts_resumes_from_saved_scan_page(self):
        """中断后重跑时，应从 Redis 保存的页码继续扫描。"""
        self.redis_client.set(self.module.REDIS_SCAN_PAGE_KEY, "5")

        with patch.object(self.module, "validate_mp_end_urls") as mock_validate, patch.object(
            self.module,
            "get_mp_response",
            return_value=Mock(),
        ) as mock_get, patch.object(
            self.module,
            "parse_mp_response",
            return_value=[
                {"title": "Movie 1", "link": "https://example.com/end-a", "year": "2026"},
                {"title": "Movie 2", "link": "https://example.com/end-b", "year": "2026"},
            ],
        ):
            self.module.enqueue_mp_posts(
                start_page=2,
                end=["https://example.com/end-a", "https://example.com/end-b"],
                redis_client=self.redis_client,
            )

        mock_validate.assert_called_once()
        mock_get.assert_called_once_with("https://example.com/movies/page/5/")
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY), "1")

    def test_enqueue_mp_posts_returns_immediately_when_scan_is_already_complete(self):
        """扫描已完成且准备续跑详情时，应跳过入队阶段。"""
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")

        with patch.object(self.module, "validate_mp_end_urls") as mock_validate, patch.object(
            self.module,
            "get_mp_response",
        ) as mock_get:
            self.module.enqueue_mp_posts(
                start_page=2,
                end=["https://example.com/end-a", "https://example.com/end-b"],
                redis_client=self.redis_client,
            )

        mock_validate.assert_not_called()
        mock_get.assert_not_called()

    def test_enqueue_mp_posts_raises_when_end_urls_are_missing(self):
        """没有截止 URL 时应直接报错，避免无限翻页。"""
        with self.assertRaisesRegex(ValueError, "截止 URL"):
            self.module.enqueue_mp_posts(start_page=2, end=[], redis_client=self.redis_client)

    def test_drain_mp_queue_keeps_failed_items_in_processing_for_next_run(self):
        """普通失败任务应保留在 processing，留待下次重跑恢复。"""
        payloads = [
            self.module._scrapy_redis.serialize_payload({"link": "https://example.com/good", "title": "Good", "year": "2026"}),
            self.module._scrapy_redis.serialize_payload({"link": "https://example.com/bad", "title": "Bad", "year": "2026"}),
        ]
        for payload in payloads:
            self.redis_client.rpush(self.module.REDIS_PENDING_KEY, payload)

        def fake_visit(item: dict):
            if item["link"] == "https://example.com/bad":
                raise RuntimeError("boom")

        with patch.object(self.module, "visit_mp_url", side_effect=fake_visit):
            self.module.drain_mp_queue(redis_client=self.redis_client)

        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 0)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PROCESSING_KEY), 1)
        failed_payload = self.module._scrapy_redis.deserialize_payload(
            self.redis_client.lrange(self.module.REDIS_PROCESSING_KEY, 0, -1)[0]
        )
        self.assertEqual(failed_payload["link"], "https://example.com/bad")

    def test_drain_mp_queue_disables_processing_recovery_on_start(self):
        """MP 详情阶段启动时，不应自动回收 processing 残留。"""
        with patch.object(self.module, "drain_queue") as mock_drain_queue:
            self.module.drain_mp_queue(redis_client=self.redis_client)

        mock_drain_queue.assert_called_once()
        self.assertFalse(mock_drain_queue.call_args.kwargs["recover_processing_on_start"])
        self.assertNotIn("failed_key", mock_drain_queue.call_args.kwargs)

    def test_drain_mp_queue_aborts_and_requeues_when_cookie_expires(self):
        """详情阶段若命中 Cloudflare 验证页，应终止本轮并把任务放回 pending。"""
        self.redis_client.rpush(
            self.module.REDIS_PENDING_KEY,
            self.module._scrapy_redis.serialize_payload(
                {"link": "https://example.com/retry", "title": "Retry", "year": "2026"}
            ),
        )

        with patch.object(
            self.module,
            "visit_mp_url",
            side_effect=self.module.MpCloudflareError("mp Cookie 已失效或触发 Cloudflare 验证"),
        ):
            with self.assertRaises(self.module.MpCloudflareError):
                self.module.drain_mp_queue(redis_client=self.redis_client)

        self.assertEqual(self.redis_client.llen(self.module.REDIS_PROCESSING_KEY), 0)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 1)

    def test_drain_mp_queue_logs_when_queue_is_empty(self):
        """pending 队列为空时应直接记录提示。"""
        with self.assertLogs(self.module.logger.name, level="INFO") as logs:
            self.module.drain_mp_queue(redis_client=self.redis_client)

        self.assertIn("MP 队列为空，没有待处理任务", logs.output[0])


class TestFinalizeMpRun(unittest.TestCase):
    """验证 MP 扫描状态清理逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_finalize_mp_run_clears_scan_state_after_all_work_finishes(self):
        """扫描完成且所有队列清空后，应删除扫描状态。"""
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")
        self.redis_client.set(self.module.REDIS_SCAN_PAGE_KEY, "9")

        self.module.finalize_mp_run(redis_client=self.redis_client)

        self.assertIsNone(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY))
        self.assertIsNone(self.redis_client.get(self.module.REDIS_SCAN_PAGE_KEY))

    def test_finalize_mp_run_keeps_processing_residue_for_next_restart(self):
        """收尾时若只剩 processing 残留，应仅警告并保留扫描状态。"""
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")
        self.redis_client.set(self.module.REDIS_SCAN_PAGE_KEY, "9")
        processing_payload = self.module._scrapy_redis.serialize_payload(
            {"link": "https://example.com/retry", "title": "Retry", "year": "2026"}
        )
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, processing_payload)

        with self.assertLogs(self.module.logger.name, level="WARNING") as logs:
            self.module.finalize_mp_run(redis_client=self.redis_client)

        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY), "1")
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_PAGE_KEY), "9")
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PROCESSING_KEY), 1)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 0)
        self.assertIn("已保留处理中队列，请直接重跑", logs.output[0])

    def test_recover_mp_processing_when_pending_is_empty_moves_processing_back(self):
        """启动时若 pending 为空且 processing 有残留，应回退到 pending。"""
        processing_payload = self.module._scrapy_redis.serialize_payload(
            {"link": "https://example.com/retry", "title": "Retry", "year": "2026"}
        )
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, processing_payload)

        with self.assertLogs(self.module.logger.name, level="WARNING") as logs:
            recovered_count = self.module.recover_mp_processing_when_pending_is_empty(self.redis_client)

        self.assertEqual(recovered_count, 1)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PROCESSING_KEY), 0)
        self.assertEqual(
            self.redis_client.lrange(self.module.REDIS_PENDING_KEY, 0, -1),
            [processing_payload],
        )
        self.assertIn("已回退到待处理队列并继续运行", logs.output[0])


class TestScrapyMpMain(unittest.TestCase):
    """验证主抓取流程的编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mp()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_mp_calls_enqueue_and_drain_with_shared_redis_client(self):
        """主入口应复用同一个 Redis 客户端串起三阶段。"""
        with patch.object(
            self.module,
            "get_redis_client",
            return_value=self.redis_client,
        ) as mock_get_redis, patch.object(
            self.module,
            "enqueue_mp_posts",
        ) as mock_enqueue, patch.object(
            self.module,
            "drain_mp_queue",
        ) as mock_drain, patch.object(
            self.module,
            "finalize_mp_run",
        ) as mock_finalize:
            self.module.scrapy_mp(start_page=2, end=["https://example.com/old"])

        mock_get_redis.assert_called_once_with()
        mock_enqueue.assert_called_once_with(
            start_page=2,
            end=["https://example.com/old"],
            redis_client=self.redis_client,
        )
        mock_drain.assert_called_once_with(redis_client=self.redis_client)
        mock_finalize.assert_called_once_with(redis_client=self.redis_client)

    def test_scrapy_mp_still_finalizes_when_drain_raises(self):
        """详情阶段抛错时，主入口仍应执行收尾逻辑。"""
        with patch.object(
            self.module,
            "get_redis_client",
            return_value=self.redis_client,
        ), patch.object(
            self.module,
            "recover_mp_processing_when_pending_is_empty",
        ), patch.object(
            self.module,
            "enqueue_mp_posts",
        ) as mock_enqueue, patch.object(
            self.module,
            "drain_mp_queue",
            side_effect=RuntimeError("boom"),
        ) as mock_drain, patch.object(
            self.module,
            "finalize_mp_run",
        ) as mock_finalize:
            with self.assertRaisesRegex(RuntimeError, "boom"):
                self.module.scrapy_mp(start_page=2, end=["https://example.com/old"])

        mock_enqueue.assert_called_once_with(
            start_page=2,
            end=["https://example.com/old"],
            redis_client=self.redis_client,
        )
        mock_drain.assert_called_once_with(redis_client=self.redis_client)
        mock_finalize.assert_called_once_with(redis_client=self.redis_client)

    def test_scrapy_mp_recovers_processing_before_run_when_pending_is_empty(self):
        """启动时若 pending 为空但 processing 有残留，应先回退再继续主流程。"""
        processing_payload = self.module._scrapy_redis.serialize_payload(
            {"link": "https://example.com/retry", "title": "Retry", "year": "2026"}
        )
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, processing_payload)

        with patch.object(
            self.module,
            "get_redis_client",
            return_value=self.redis_client,
        ), patch.object(
            self.module,
            "enqueue_mp_posts",
        ) as mock_enqueue, patch.object(
            self.module,
            "drain_mp_queue",
        ) as mock_drain, patch.object(
            self.module,
            "finalize_mp_run",
        ) as mock_finalize:
            self.module.scrapy_mp(start_page=2, end=["https://example.com/old"])

        self.assertEqual(self.redis_client.llen(self.module.REDIS_PROCESSING_KEY), 0)
        self.assertEqual(
            self.redis_client.lrange(self.module.REDIS_PENDING_KEY, 0, -1),
            [processing_payload],
        )
        mock_enqueue.assert_called_once_with(
            start_page=2,
            end=["https://example.com/old"],
            redis_client=self.redis_client,
        )
        mock_drain.assert_called_once_with(redis_client=self.redis_client)
        mock_finalize.assert_called_once_with(redis_client=self.redis_client)


if __name__ == "__main__":
    unittest.main()
