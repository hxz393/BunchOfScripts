"""
针对 ``my_scripts.scrapy_rls`` 的单元测试。

这里不依赖真实配置文件，也不会发出真实网络请求。
目标是只验证抓取脚本的核心行为：
1. 截止标题读取、列表页请求和 HTML 解析。
2. 两条列表流程顺序入 Redis 队列，详情阶段统一消费。
3. 详情页 IMDb 提取、文件名组装和最终配置回写。
"""

import copy
import importlib.util
import json
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


class FakeRedis:
    """用于测试 RLS Redis 队列流程的内存实现。"""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.sets = {}
        self.lists = {}
        self.values = {}

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


def load_scrapy_rls(config: dict | None = None):
    """
    在隔离环境中加载 ``scrapy_rls`` 模块。

    被测模块在 import 时就会读取配置并导入 ``my_module`` / ``retrying`` /
    ``redis`` / ``scrapy_redis``，这里先注入假的依赖，避免测试依赖真实环境。
    """
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "rls_url": "https://example.com/",
        "rls_verification_url": "https://example.com/verify",
        "rls_cookie": "cookie=value",
        "request_head": {"User-Agent": "unit-test"},
        "output_dir": temp_dir.name,
        "foreign_end_titles": ["Foreign Stop 2026"],
        "movie_end_titles": ["Movie Stop 2026"],
        "thread_number": 4,
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
    fake_my_module.update_json_config = lambda _path, _key, _value: None

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    fake_redis = types.ModuleType("redis")
    fake_redis.Redis = FakeRedis

    fake_scrapy_redis = types.ModuleType("scrapy_redis")
    fake_scrapy_redis.get_redis_client = lambda: FakeRedis()
    fake_scrapy_redis.serialize_payload = lambda payload: json.dumps(payload, ensure_ascii=False, sort_keys=True)
    fake_scrapy_redis.deserialize_payload = lambda payload: json.loads(payload)

    def fake_push_items_to_queue(redis_client, items, *, seen_key, pending_key, unique_value, serializer):
        if not items:
            return 0
        added_count = 0
        for item in items:
            if redis_client.sadd(seen_key, unique_value(item)):
                redis_client.rpush(pending_key, serializer(item))
                added_count += 1
        return added_count

    fake_scrapy_redis.push_items_to_queue = fake_push_items_to_queue
    fake_scrapy_redis.drain_queue = lambda *args, **kwargs: {"processed": 0, "success": 0, "failed": 0}

    fake_sort_movie_ops = types.ModuleType("sort_movie_ops")

    def fake_extract_imdb_id_from_links(hrefs):
        fallback = None
        for href in hrefs:
            if "imdb.com/title/" in href:
                return href.split("/title/")[1].split("/")[0].lower()
            if fallback is None:
                match = __import__("re").search(r"(tt\d+)", href, __import__("re").IGNORECASE)
                if match:
                    fallback = match.group(1).lower()
        return fallback

    fake_sort_movie_ops.extract_imdb_id_from_links = fake_extract_imdb_id_from_links

    spec = importlib.util.spec_from_file_location(
        f"scrapy_rls_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "retrying": fake_retrying,
            "redis": fake_redis,
            "scrapy_redis": fake_scrapy_redis,
            "sort_movie_ops": fake_sort_movie_ops,
        },
    ):
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


class TestRlsHelpers(unittest.TestCase):
    """验证截止标题读取与列表页解析。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rls(
            {
                "foreign_end_titles": ["Foreign Stop 2026", "Foreign Backup ⭐ 2026"],
                "movie_end_titles": ["Movie Stop 2026"],
            }
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_current_end_titles_normalizes_per_mode_values(self):
        """两条流程应分别读取并清洗各自的截止标题。"""
        self.assertEqual(
            self.module.get_current_end_titles(True),
            ["Foreign.Stop.2026", "Foreign.Backup.2026"],
        )
        self.assertEqual(self.module.get_current_end_titles(False), ["Movie.Stop.2026"])

    def test_parse_rls_response_returns_titles_and_urls(self):
        """应提取标题、链接，并清理星标和空格。"""
        response = Mock(
            text=build_rls_page_html(
                build_rls_item_html("Movie Title ⭐ 2026", "https://example.com/post/1"),
                build_rls_item_html("Another Title 2025", "https://example.com/post/2"),
            )
        )

        self.assertEqual(
            self.module.parse_rls_response(response),
            [
                {"title": "Movie.Title.2026", "url": "https://example.com/post/1"},
                {"title": "Another.Title.2025", "url": "https://example.com/post/2"},
            ],
        )

    def test_extract_rls_imdb_id_prefers_canonical_link_then_falls_back_to_loose_tt(self):
        """应优先使用标准 IMDb 链接，缺失时再回退到宽松 ``tt`` 提取。"""
        soup = self.module.BeautifulSoup(
            build_detail_page_html(
                "https://example.com/jump?target=tt1234567",
                "https://www.imdb.com/title/tt7654321/",
            ),
            "lxml",
        )
        self.assertEqual(self.module.extract_rls_imdb_id(soup), "tt7654321")

        soup = self.module.BeautifulSoup(
            build_detail_page_html("https://example.com/jump?target=tt1234567"),
            "lxml",
        )
        self.assertEqual(self.module.extract_rls_imdb_id(soup), "tt1234567")


class TestGetRlsResponse(unittest.TestCase):
    """验证单页请求逻辑。"""

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
        response.text = ""

        with patch.object(self.module.requests, "get", return_value=response):
            with self.assertRaisesRegex(SystemExit, "403"):
                self.module.get_rls_response("https://example.com/page/1")

    def test_get_rls_response_raises_cloudflare_error_when_challenge_page_is_detected(self):
        """命中 Cloudflare 验证页时，应抛出致命错误而不是继续重试。"""
        response = Mock(status_code=200)
        response.text = "<title>Just a moment...</title>"

        with patch.object(self.module.requests, "get", return_value=response):
            with self.assertRaisesRegex(self.module.RlsCloudflareError, "https://example.com/verify"):
                self.module.get_rls_response("https://example.com/page/1")

    def test_get_rls_response_raises_exception_for_non_200_status(self):
        """普通非 200 状态码应抛异常，交给重试逻辑处理。"""
        response = Mock(status_code=500)
        response.text = ""

        with patch.object(self.module.requests, "get", return_value=response):
            with self.assertRaisesRegex(Exception, "500"):
                self.module.get_rls_response("https://example.com/page/1")


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
        response = Mock(text=build_detail_page_html("https://example.com/jump?target=tt1234567"))

        with patch.object(self.module, "get_rls_response", return_value=response):
            self.module.visit_rls_url(result_item)

        output_path = Path(self.temp_dir.name) / "Loose.Match.2026 - rls [tt1234567].rls"
        self.assertEqual(result_item["imdb"], "tt1234567")
        self.assertTrue(output_path.exists())


class TestEnqueueRlsPosts(unittest.TestCase):
    """验证两条列表流程统一入队。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rls(
            {
                "foreign_end_titles": ["Foreign Stop 2026"],
                "movie_end_titles": ["Movie Stop 2026"],
            }
        )
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_enqueue_rls_single_mode_retries_empty_page_and_marks_complete(self):
        """当前页解析为空时应重试，命中截止标题后应记录下一轮标题并标记完成。"""
        parsed_page = [
            {"title": "Fresh.Release.2026", "url": "u1"},
            {"title": "Foreign.Stop.2026", "url": "u2"},
        ]

        with patch.object(self.module, "get_rls_response", return_value=Mock(text="html")) as mock_get, patch.object(
            self.module, "parse_rls_response", side_effect=[[], parsed_page]
        ) as mock_parse, patch.object(
            self.module.time, "sleep"
        ) as mock_sleep:
            self.module.enqueue_rls_single_mode(2, True, self.redis_client)

        self.assertEqual(
            mock_get.call_args_list,
            [
                call("https://example.com/category/foreign-movies/page/2/?s="),
                call("https://example.com/category/foreign-movies/page/2/?s="),
            ],
        )
        self.assertEqual(mock_parse.call_count, 2)
        mock_sleep.assert_called_once_with(3)
        self.assertEqual(self.redis_client.get(self.module.REDIS_FOREIGN_SCAN_COMPLETE_KEY), "1")
        self.assertEqual(self.redis_client.get(self.module.REDIS_FOREIGN_SCAN_PAGE_KEY), "3")
        queued_payloads = self.redis_client.lrange(self.module.REDIS_PENDING_KEY, 0, -1)
        queued_urls = [self.module.deserialize_payload(payload)["url"] for payload in queued_payloads]
        self.assertEqual(queued_urls, ["u1", "u2"])
        self.assertEqual(
            self.module.deserialize_payload(self.redis_client.get(self.module.REDIS_FOREIGN_NEXT_END_TITLES_KEY))["titles"],
            ["Fresh.Release.2026", "Foreign.Stop.2026"],
        )

    def test_enqueue_rls_single_mode_skips_when_scan_is_already_complete(self):
        """扫描已完成时，应直接跳过当前模式的入队。"""
        self.redis_client.set(self.module.REDIS_FOREIGN_SCAN_COMPLETE_KEY, "1")

        with patch.object(self.module, "get_rls_response") as mock_get:
            self.module.enqueue_rls_single_mode(2, True, self.redis_client)

        mock_get.assert_not_called()
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 0)

    def test_enqueue_rls_single_mode_stops_after_reaching_empty_page_retry_limit(self):
        """同一页连续空结果达到上限后，应抛错停止而不是无限重试。"""
        with patch.object(self.module, "get_rls_response", return_value=Mock(text="html")) as mock_get, patch.object(
            self.module, "parse_rls_response", side_effect=[[], [], []]
        ), patch.object(
            self.module.time, "sleep"
        ) as mock_sleep:
            with self.assertRaisesRegex(RuntimeError, "连续 3 次解析为空"):
                self.module.enqueue_rls_single_mode(2, True, self.redis_client)

        self.assertEqual(mock_get.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)
        self.assertEqual(self.redis_client.get(self.module.REDIS_FOREIGN_SCAN_PAGE_KEY), "2")
        self.assertIsNone(self.redis_client.get(self.module.REDIS_FOREIGN_SCAN_COMPLETE_KEY))

    def test_enqueue_rls_single_mode_resumes_from_saved_page(self):
        """存在扫描断点时，应从 Redis 记录的页码继续。"""
        self.redis_client.set(self.module.REDIS_MOVIE_SCAN_PAGE_KEY, "5")
        parsed_page = [
            {"title": "Movie.Stop.2026", "url": "u2"},
            {"title": "Movie.New.2026", "url": "u3"},
        ]

        with patch.object(self.module, "get_rls_response", return_value=Mock(text="html")) as mock_get, patch.object(
            self.module, "parse_rls_response", return_value=parsed_page
        ):
            self.module.enqueue_rls_single_mode(1, False, self.redis_client)

        mock_get.assert_called_once_with("https://example.com/category/movies/page/5/?s=")
        self.assertEqual(self.redis_client.get(self.module.REDIS_MOVIE_SCAN_PAGE_KEY), "6")
        self.assertEqual(self.redis_client.get(self.module.REDIS_MOVIE_SCAN_COMPLETE_KEY), "1")

    def test_enqueue_rls_posts_runs_both_modes_and_deduplicates_urls_in_shared_queue(self):
        """两条列表流程应顺序入同一个队列，重复 URL 只保留一份。"""
        foreign_page = [
            {"title": "Fresh.Release.2026", "url": "u1"},
            {"title": "Foreign.Stop.2026", "url": "u2"},
        ]
        movie_page = [
            {"title": "Movie.Stop.2026", "url": "u2"},
            {"title": "Movie.New.2026", "url": "u3"},
        ]

        with patch.object(self.module, "get_rls_response", return_value=Mock(text="html")), patch.object(
            self.module, "parse_rls_response", side_effect=[foreign_page, movie_page]
        ):
            self.module.enqueue_rls_posts(start_page=1, redis_client=self.redis_client)

        queued_payloads = self.redis_client.lrange(self.module.REDIS_PENDING_KEY, 0, -1)
        queued_urls = [self.module.deserialize_payload(payload)["url"] for payload in queued_payloads]
        self.assertEqual(queued_urls, ["u1", "u2", "u3"])
        self.assertEqual(self.redis_client.get(self.module.REDIS_FOREIGN_SCAN_COMPLETE_KEY), "1")
        self.assertEqual(self.redis_client.get(self.module.REDIS_MOVIE_SCAN_COMPLETE_KEY), "1")


class TestDrainRlsQueue(unittest.TestCase):
    """验证统一详情消费阶段。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rls()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_recover_rls_processing_when_pending_is_empty_moves_processing_back(self):
        """待处理为空但处理中有残留时，应回退到待处理并继续运行。"""
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, "payload-1")
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, "payload-2")

        with patch.object(self.module.logger, "warning") as mock_warning:
            recovered_count = self.module.recover_rls_processing_when_pending_is_empty(self.redis_client)

        self.assertEqual(recovered_count, 2)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PROCESSING_KEY), 0)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 2)
        mock_warning.assert_called_once_with("RLS 检测到待处理为空但处理中残留 2 条，已回退到待处理队列并继续运行")

    def test_recover_rls_processing_when_pending_exists_skips_recovery(self):
        """只要待处理不为空，就不应回退 processing。"""
        self.redis_client.rpush(self.module.REDIS_PENDING_KEY, "payload-pending")
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, "payload-processing")

        recovered_count = self.module.recover_rls_processing_when_pending_is_empty(self.redis_client)

        self.assertEqual(recovered_count, 0)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 1)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PROCESSING_KEY), 1)

    def test_drain_rls_queue_uses_shared_queue_without_processing_recovery(self):
        """统一消费阶段应直接处理待处理队列，并启用致命错误中止策略。"""
        self.redis_client.rpush(self.module.REDIS_PENDING_KEY, self.module.serialize_payload({"title": "A", "url": "u1"}))

        with patch.object(self.module, "drain_queue") as mock_drain:
            self.module.drain_rls_queue(redis_client=self.redis_client)

        mock_drain.assert_called_once_with(
            self.redis_client,
            pending_key=self.module.REDIS_PENDING_KEY,
            processing_key=self.module.REDIS_PROCESSING_KEY,
            max_workers=self.module.THREAD_NUMBER,
            worker=self.module.visit_rls_url,
            deserialize=self.module.deserialize_payload,
            logger=self.module.logger,
            queue_label="RLS",
            identify_item=unittest.mock.ANY,
            abort_on_exception=unittest.mock.ANY,
            recover_processing_on_start=False,
            keep_failed_in_processing=True,
        )

        abort_on_exception = mock_drain.call_args.kwargs["abort_on_exception"]
        self.assertTrue(abort_on_exception(self.module.RlsCloudflareError("cf")))
        self.assertFalse(abort_on_exception(RuntimeError("boom")))


class TestFinalizeRlsRun(unittest.TestCase):
    """验证最终配置回写和状态清理。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rls()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_finalize_rls_run_updates_both_end_title_lists_and_clears_state(self):
        """两条列表都扫完且队列清空后，应一次性回写两套截止标题并清理运行态，但保留 seen。"""
        self.redis_client.set(self.module.REDIS_FOREIGN_SCAN_COMPLETE_KEY, "1")
        self.redis_client.set(self.module.REDIS_MOVIE_SCAN_COMPLETE_KEY, "1")
        self.redis_client.set(
            self.module.REDIS_FOREIGN_NEXT_END_TITLES_KEY,
            self.module.serialize_payload({"titles": ["F1", "F2"]}),
        )
        self.redis_client.set(
            self.module.REDIS_MOVIE_NEXT_END_TITLES_KEY,
            self.module.serialize_payload({"titles": ["M1", "M2"]}),
        )
        self.redis_client.set(self.module.REDIS_FOREIGN_SCAN_PAGE_KEY, "3")
        self.redis_client.set(self.module.REDIS_MOVIE_SCAN_PAGE_KEY, "4")
        self.redis_client.sadd(self.module.REDIS_SEEN_KEY, "u1")

        with patch.object(self.module, "update_json_config") as mock_update:
            self.module.finalize_rls_run(redis_client=self.redis_client)

        self.assertEqual(
            mock_update.call_args_list,
            [
                call(self.module.CONFIG_PATH, "foreign_end_titles", ["F1", "F2"]),
                call(self.module.CONFIG_PATH, "movie_end_titles", ["M1", "M2"]),
            ],
        )
        self.assertIsNone(self.redis_client.get(self.module.REDIS_FOREIGN_SCAN_COMPLETE_KEY))
        self.assertIsNone(self.redis_client.get(self.module.REDIS_MOVIE_SCAN_COMPLETE_KEY))
        self.assertEqual(self.redis_client.sets[self.module.REDIS_SEEN_KEY], {"u1"})

    def test_finalize_rls_run_skips_update_when_scan_is_incomplete(self):
        """任一列表流程未完成时，不应提前回写配置。"""
        self.redis_client.set(self.module.REDIS_FOREIGN_SCAN_COMPLETE_KEY, "1")

        with patch.object(self.module, "update_json_config") as mock_update:
            self.module.finalize_rls_run(redis_client=self.redis_client)

        mock_update.assert_not_called()

    def test_finalize_rls_run_skips_update_when_pending_tasks_remain(self):
        """待处理队列未清空时，不应提前回写配置。"""
        self.redis_client.set(self.module.REDIS_FOREIGN_SCAN_COMPLETE_KEY, "1")
        self.redis_client.set(self.module.REDIS_MOVIE_SCAN_COMPLETE_KEY, "1")
        self.redis_client.rpush(self.module.REDIS_PENDING_KEY, "payload-1")

        with patch.object(self.module, "update_json_config") as mock_update:
            self.module.finalize_rls_run(redis_client=self.redis_client)

        mock_update.assert_not_called()
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 1)

    def test_finalize_rls_run_warns_when_processing_tasks_remain(self):
        """待处理已空但 processing 仍有任务时，应保留 processing 并提示直接重跑。"""
        self.redis_client.set(self.module.REDIS_FOREIGN_SCAN_COMPLETE_KEY, "1")
        self.redis_client.set(self.module.REDIS_MOVIE_SCAN_COMPLETE_KEY, "1")
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, "payload-1")

        with patch.object(self.module, "update_json_config") as mock_update, patch.object(
            self.module.logger, "warning"
        ) as mock_warning:
            self.module.finalize_rls_run(redis_client=self.redis_client)

        mock_update.assert_not_called()
        mock_warning.assert_called_once_with("RLS 待处理已空，但处理中仍有 1 条，已保留处理中队列，请直接重跑")


class TestScrapyRlsEntrypoint(unittest.TestCase):
    """验证主入口编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_rls()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_rls_calls_recover_enqueue_drain_and_finalize_with_shared_redis_client(self):
        """入口函数应先恢复残留，再执行入队、消费和收尾，并复用同一个 Redis 客户端。"""
        with patch.object(self.module, "get_redis_client", return_value=self.redis_client) as mock_get_redis, patch.object(
            self.module, "recover_rls_processing_when_pending_is_empty"
        ) as mock_recover, patch.object(
            self.module, "enqueue_rls_posts"
        ) as mock_enqueue, patch.object(
            self.module, "drain_rls_queue"
        ) as mock_drain, patch.object(
            self.module, "finalize_rls_run"
        ) as mock_finalize:
            self.module.scrapy_rls(start_page=2)

        mock_get_redis.assert_called_once_with()
        mock_recover.assert_called_once_with(self.redis_client)
        mock_enqueue.assert_called_once_with(start_page=2, redis_client=self.redis_client)
        mock_drain.assert_called_once_with(redis_client=self.redis_client)
        mock_finalize.assert_called_once_with(redis_client=self.redis_client)

    def test_scrapy_rls_still_finalizes_when_drain_queue_raises(self):
        """消费阶段抛错时，收尾逻辑仍应在 finally 中执行。"""
        with patch.object(self.module, "get_redis_client", return_value=self.redis_client), patch.object(
            self.module, "recover_rls_processing_when_pending_is_empty"
        ) as mock_recover, patch.object(
            self.module, "enqueue_rls_posts"
        ) as mock_enqueue, patch.object(
            self.module, "drain_rls_queue", side_effect=self.module.RlsCloudflareError("cf boom")
        ) as mock_drain, patch.object(
            self.module, "finalize_rls_run"
        ) as mock_finalize:
            with self.assertRaisesRegex(self.module.RlsCloudflareError, "cf boom"):
                self.module.scrapy_rls(start_page=5)

        mock_recover.assert_called_once_with(self.redis_client)
        mock_enqueue.assert_called_once_with(start_page=5, redis_client=self.redis_client)
        mock_drain.assert_called_once_with(redis_client=self.redis_client)
        mock_finalize.assert_called_once_with(redis_client=self.redis_client)


if __name__ == "__main__":
    unittest.main()
