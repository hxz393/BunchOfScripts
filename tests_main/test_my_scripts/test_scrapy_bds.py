"""
针对 ``my_scripts.scrapy_bds`` 的单元测试。

这里不依赖真实配置文件，也不会发出真实网络请求。
目标是只验证抓取脚本的核心行为：
1. 模块导入时的配置注入与会话初始化。
2. 请求重试、列表页解析和帖子详情写盘逻辑。
3. 主入口的分页停止、去重和多栏目编排。
"""

import copy
import datetime
import importlib.util
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, call, patch

import requests
import requests.adapters
import urllib3.util.retry
try:
    import fakeredis
except ImportError:  # pragma: no cover - 由依赖安装状态决定
    fakeredis = None

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_bds.py"


class FakeRetry:
    """最小 Retry 实现，只用于承接初始化参数。"""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class FakeHTTPAdapter:
    """最小 HTTPAdapter 实现，用于避开真实 urllib3 兼容性差异。"""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class FakeSession:
    """最小 requests.Session 实现。"""

    def __init__(self):
        self.proxies = {}
        self.mount_calls = []

    def mount(self, prefix: str, adapter):
        self.mount_calls.append((prefix, adapter))

    def get(self, *args, **kwargs):  # pragma: no cover - 测试中会按需打桩
        raise AssertionError("session.get should be patched in tests")


class _FallbackFakeRedisPipeline:
    """最小 Redis pipeline 实现。"""

    def __init__(self, client):
        self.client = client
        self.commands = []

    def sismember(self, key: str, value: str):
        self.commands.append(("sismember", key, value))
        return self

    def sadd(self, key: str, value: str):
        self.commands.append(("sadd", key, value))
        return self

    def execute(self):
        results = []
        for command, key, value in self.commands:
            results.append(getattr(self.client, command)(key, value))
        self.commands.clear()
        return results


class _FallbackFakeRedis:
    """fakeredis 不可用时使用的最小内存 Redis 实现。"""

    def __init__(self):
        self.sets = {}

    def pipeline(self):
        return _FallbackFakeRedisPipeline(self)

    def sadd(self, key: str, value: str) -> int:
        members = self.sets.setdefault(key, set())
        if value in members:
            return 0
        members.add(value)
        return 1

    def sismember(self, key: str, value: str) -> bool:
        return value in self.sets.get(key, set())

    def smembers(self, key: str) -> set[str]:
        return self.sets.get(key, set()).copy()


if fakeredis is None:
    class FakeRedis(_FallbackFakeRedis):
        """回退到手写内存 Redis。"""

else:
    class FakeRedis(fakeredis.FakeRedis):
        """优先使用带真实命令语义的 fakeredis。"""

        def __init__(self, *args, **kwargs):
            super().__init__(decode_responses=True)


def fake_retry(*args, **kwargs):
    """最小 retry 装饰器实现，支持按最大次数重试。"""
    max_attempts = kwargs.get("stop_max_attempt_number", 1)

    def decorator(func):
        def wrapper(*func_args, **func_kwargs):
            last_exception = None
            for _ in range(max_attempts):
                try:
                    return func(*func_args, **func_kwargs)
                except Exception as exc:  # pragma: no cover - 仅用于模拟第三方重试
                    last_exception = exc
            raise last_exception

        return wrapper

    return decorator


def load_scrapy_bds(config: dict | None = None):
    """
    在隔离环境中加载 ``scrapy_bds`` 模块。

    被测模块在 import 时就会读取配置并初始化 ``Retry`` / ``HTTPAdapter`` /
    ``requests.Session``，所以这里先注入假的依赖，避免测试依赖真实环境。
    """
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "group_dict": {"电影": 10},
        "output_dir": str(Path(temp_dir.name) / "downloads"),
        "bds_url": "https://example.com/",
        "bds_cookie": "cookie=value",
        "request_head": {"User-Agent": "unit-test"},
        "thread_number": 6,
        "request_interval_seconds": 0.05,
        "end_time": "2020-09-21",
        "redis_seen_key": "bds_seen",
    }
    if config:
        module_config.update(config)

    Path(module_config["output_dir"]).mkdir(parents=True, exist_ok=True)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: copy.deepcopy(module_config)
    fake_my_module.sanitize_filename = lambda filename: filename
    fake_my_module.update_json_config = lambda _file_path, _key, _value: None

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = fake_retry

    fake_redis_client = FakeRedis()
    fake_scrapy_redis = types.ModuleType("scrapy_redis")
    fake_scrapy_redis.get_redis_client = lambda: fake_redis_client

    spec = importlib.util.spec_from_file_location(
        f"scrapy_bds_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "retrying": fake_retrying,
            "scrapy_redis": fake_scrapy_redis,
        },
    ), patch.object(
        requests.adapters, "HTTPAdapter", FakeHTTPAdapter
    ), patch.object(
        requests, "Session", FakeSession
    ), patch.object(
        urllib3.util.retry, "Retry", FakeRetry
    ):
        spec.loader.exec_module(module)

    module._fake_redis_client = fake_redis_client
    return module, temp_dir


def build_thread_tbody(
        title: str = "帖子标题",
        href: str = "thread-1-1-1.html",
        date_text: str | None = "2024-01-05",
        include_th: bool = True,
        include_anchor: bool = True,
) -> str:
    """构造一条最小可用的论坛帖子 HTML。"""
    if not include_th:
        th_html = ""
    elif include_anchor:
        th_html = f'<th><a class="s xst" href="{href}">{title}</a></th>'
    else:
        th_html = "<th></th>"

    td_by_html = ""
    if date_text is not None:
        td_by_html = f'<td class="by"><span>{date_text}</span></td>'

    return f"<tbody>{th_html}{td_by_html}</tbody>"


def build_forum_html(*tbodys: str) -> bytes:
    """把若干帖子块拼成最小可用页面。"""
    html = f'<table id="threadlisttableid">{"".join(tbodys)}</table>'
    return html.encode("utf-8")


def make_forum_response(*tbodys: str):
    """构造同时带 ``text`` 与 ``content`` 的列表页响应。"""
    html_text = f'<table id="threadlisttableid">{"".join(tbodys)}</table>'
    return Mock(text=html_text, content=html_text.encode("utf-8"))


class TestModuleLoad(unittest.TestCase):
    """验证模块导入时的配置注入。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_bds()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_scrapy_bds_injects_cookie_and_initializes_session(self):
        """模块加载时应把 cookie 注入请求头，并初始化代理与挂载。"""
        self.assertEqual(self.module.REQUEST_HEAD["Cookie"], "cookie=value")
        self.assertEqual(self.module.THREAD_NUMBER, 6)
        self.assertEqual(self.module.REQUEST_INTERVAL_SECONDS, 0.05)
        self.assertEqual(
            self.module.session.proxies,
            {
                "http": "http://127.0.0.1:7890",
                "https": "http://127.0.0.1:7890",
            },
        )
        self.assertEqual(
            [prefix for prefix, _adapter in self.module.session.mount_calls],
            ["http://", "https://"],
        )

    def test_create_retry_strategy_falls_back_to_method_whitelist_for_legacy_urllib3(self):
        """旧版 urllib3 不支持 ``allowed_methods`` 时，应回退到 ``method_whitelist``。"""

        def fake_retry_factory(**kwargs):
            if "allowed_methods" in kwargs:
                raise TypeError("unsupported")
            return kwargs

        with patch.object(self.module, "Retry", side_effect=fake_retry_factory):
            retry_strategy = self.module.create_retry_strategy()

        self.assertEqual(retry_strategy["method_whitelist"], ["POST", "GET"])
        self.assertEqual(retry_strategy["status_forcelist"], [502])


class TestEndTimeHelpers(unittest.TestCase):
    """验证 ``end_time`` 的配置读取和回写辅助逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_bds({"end_time": "2024-01-01"})

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_current_end_time_reloads_value_from_config(self):
        """应每次从配置读取最新 ``end_time``，而不是只使用模块导入时的快照。"""
        with patch.object(self.module, "read_json_to_dict", return_value={"end_time": "2024-03-25"}):
            result = self.module.get_current_end_time()

        self.assertEqual(result, "2024-03-25")

    def test_get_yesterday_date_str_returns_previous_day_of_reference_date(self):
        """``end_time`` 更新值应来自当天前一天。"""
        result = self.module.get_yesterday_date_str(datetime.date(2024, 1, 10))

        self.assertEqual(result, "2024-01-09")

    def test_parse_bds_date_returns_none_for_invalid_string(self):
        """无法解析的日期字符串应返回 ``None``。"""
        self.assertIsNone(self.module.parse_bds_date("昨天"))

    def test_finalize_bds_run_updates_config_when_run_succeeds(self):
        """无失败时，应把配置回写为昨天，而不是从帖子日期推导。"""
        with patch.object(self.module, "get_yesterday_date_str", return_value="2024-01-09"), patch.object(
            self.module, "update_json_config"
        ) as mock_update:
            self.module.finalize_bds_run(had_failures=False)

        mock_update.assert_called_once_with("config/scrapy_bds.json", "end_time", "2024-01-09")

    def test_finalize_bds_run_skips_update_when_any_detail_task_failed(self):
        """详情任务失败时，不应推进 ``end_time``，以免遗漏下次补抓。"""
        with patch.object(self.module, "update_json_config") as mock_update:
            self.module.finalize_bds_run(had_failures=True)

        mock_update.assert_not_called()


class TestSeenQueueHelpers(unittest.TestCase):
    """验证 BDS 只保留 seen 集合的去重逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_bds()
        self.redis_client = self.module._fake_redis_client

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_filter_seen_items_skips_urls_already_recorded_in_seen_set(self):
        """已存在于 seen 集合的 URL 不应再次进入详情抓取阶段。"""
        self.redis_client.sadd(self.module.REDIS_SEEN_KEY, "https://example.com/t/1")

        result = self.module.filter_seen_items(
            [
                {"title": "A", "link": "https://example.com/t/1", "date": "2024-01-03"},
                {"title": "B", "link": "https://example.com/t/2", "date": "2024-01-02"},
            ],
            redis_client=self.redis_client,
        )

        self.assertEqual(
            result,
            [{"title": "B", "link": "https://example.com/t/2", "date": "2024-01-02"}],
        )

    def test_mark_seen_url_adds_successful_url_to_seen_set(self):
        """成功抓取后的 URL 应写入 seen 集合，供后续去重。"""
        self.module.mark_seen_url("https://example.com/t/1", redis_client=self.redis_client)

        self.assertEqual(
            self.redis_client.smembers(self.module.REDIS_SEEN_KEY),
            {"https://example.com/t/1"},
        )

    def test_append_page_results_stops_on_duplicate_link_even_when_other_fields_changed(self):
        """本轮翻页去重应按 URL，而不是按整个字典比较。"""
        all_results = []
        seen_links = set()

        stop = self.module.append_page_results(
            all_results,
            seen_links,
            [
                {"title": "A", "link": "https://example.com/t/1", "date": "2024-01-03"},
            ],
        )
        self.assertFalse(stop)

        stop = self.module.append_page_results(
            all_results,
            seen_links,
            [
                {"title": "A (edited)", "link": "https://example.com/t/1", "date": "2024-01-02"},
                {"title": "B", "link": "https://example.com/t/2", "date": "2024-01-01"},
            ],
        )

        self.assertTrue(stop)
        self.assertEqual(
            all_results,
            [{"title": "A", "link": "https://example.com/t/1", "date": "2024-01-03"}],
        )


class TestGetBdsResponse(unittest.TestCase):
    """验证单页请求逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_bds()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_bds_response_returns_response_and_sets_utf8_encoding(self):
        """请求成功时应返回响应对象，并统一设置 UTF-8 编码。"""
        response = Mock(status_code=200, text="A" * 10001)

        with patch.object(self.module.session, "get", return_value=response) as mock_get:
            result = self.module.get_bds_response("https://example.com/thread/1")

        self.assertIs(result, response)
        self.assertEqual(response.encoding, "utf-8")
        mock_get.assert_called_once_with(
            "https://example.com/thread/1",
            timeout=30,
            verify=False,
            headers=self.module.REQUEST_HEAD,
        )

    def test_get_bds_response_retries_and_raises_when_status_code_is_not_200(self):
        """状态码持续异常时，应按重试次数耗尽后抛错。"""
        response = Mock(status_code=503, text="A" * 10001)

        with patch.object(self.module.session, "get", return_value=response) as mock_get, patch.object(
            self.module.logger, "error"
        ) as mock_error:
            with self.assertRaisesRegex(Exception, "请求失败"):
                self.module.get_bds_response("https://example.com/thread/1")

        self.assertEqual(mock_get.call_count, 15)
        self.assertEqual(mock_error.call_count, 15)

    def test_get_bds_response_retries_and_raises_when_page_is_too_short(self):
        """返回正文过短时，应视为被封锁并重试到耗尽。"""
        response = Mock(status_code=200, text="too short")

        with patch.object(self.module.session, "get", return_value=response) as mock_get, patch.object(
            self.module.logger, "error"
        ) as mock_error:
            with self.assertRaisesRegex(Exception, "请求被封锁"):
                self.module.get_bds_response("https://example.com/thread/1")

        self.assertEqual(mock_get.call_count, 15)
        self.assertEqual(mock_error.call_count, 15)


class TestParseForumPage(unittest.TestCase):
    """验证论坛列表页解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_bds()
        self.stop_time = datetime.datetime(2024, 1, 1)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_forum_page_collects_new_and_undated_posts_on_first_page(self):
        """第一页应保留新帖和无法解析日期的帖子，旧帖仅跳过不触发停止。"""
        response = make_forum_response(
            build_thread_tbody(title="新帖子", href="thread-100-1-1.html", date_text="2024-01-05"),
            build_thread_tbody(title="旧帖子", href="thread-101-1-1.html", date_text="2023-12-30"),
            build_thread_tbody(title="昨天帖子", href="thread-102-1-1.html", date_text="昨天"),
            build_thread_tbody(include_th=False),
            build_thread_tbody(include_anchor=False),
        )

        with patch.object(self.module, "get_bds_response", return_value=response) as mock_get:
            result, stop = self.module.parse_forum_page(group_id=10, start_page=1, stop_time=self.stop_time)

        self.assertEqual(
            result,
            [
                {
                    "title": "新帖子",
                    "link": "https://example.com/thread-100-1-1.html",
                    "date": "2024-01-05",
                },
                {
                    "title": "昨天帖子",
                    "link": "https://example.com/thread-102-1-1.html",
                    "date": "昨天",
                },
            ],
        )
        self.assertFalse(stop)
        mock_get.assert_called_once_with("https://example.com/forum.php?mod=forumdisplay&fid=10&page=1")

    def test_parse_forum_page_sets_stop_when_old_post_appears_after_first_page(self):
        """第二页及之后出现早于停止日期的帖子时，应返回停止标记。"""
        response = make_forum_response(
            build_thread_tbody(title="仍然保留", href="thread-200-1-1.html", date_text="2024-01-03"),
            build_thread_tbody(title="触发停止", href="thread-201-1-1.html", date_text="2023-12-31"),
        )

        with patch.object(self.module, "get_bds_response", return_value=response):
            result, stop = self.module.parse_forum_page(group_id=10, start_page=2, stop_time=self.stop_time)

        self.assertEqual(
            result,
            [
                {
                    "title": "仍然保留",
                    "link": "https://example.com/thread-200-1-1.html",
                    "date": "2024-01-03",
                }
            ],
        )
        self.assertTrue(stop)

    def test_parse_forum_page_returns_empty_when_thread_table_is_missing(self):
        """页面缺少帖子表格时，应记录错误并返回空结果。"""
        response = Mock(text="<html><body><div>blocked</div></body></html>", content=b"<html><body><div>blocked</div></body></html>")

        with patch.object(self.module, "get_bds_response", return_value=response), self.assertLogs(
            self.module.logger.name, level="ERROR"
        ) as logs:
            result, stop = self.module.parse_forum_page(group_id=10, start_page=1, stop_time=self.stop_time)

        self.assertEqual(result, [])
        self.assertFalse(stop)
        self.assertIn("没有找到帖子", logs.output[0])

    def test_parse_forum_page_raises_when_anchor_href_is_missing(self):
        """帖子标题链接缺少 href 时，应中止整页抓取。"""
        broken_html = (
            build_thread_tbody(title="坏帖子", href="thread-300-1-1.html").replace(' href="thread-300-1-1.html"', ""),
        )
        html_text = f'<table id="threadlisttableid">{"".join(broken_html)}</table>'
        response = Mock(text=html_text, content=html_text.encode("utf-8"))

        with patch.object(self.module, "get_bds_response", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "缺少 href"):
                self.module.parse_forum_page(group_id=10, start_page=1, stop_time=self.stop_time)

    def test_parse_forum_tbody_returns_none_when_required_nodes_are_missing(self):
        """缺少 ``th`` 或标题链接时，应返回 ``None``。"""
        no_th = Mock()
        no_th.find.return_value = None

        html = build_thread_tbody(include_anchor=False)
        tbody = self.module.BeautifulSoup(html, "html.parser").find("tbody")

        self.assertIsNone(self.module.parse_forum_tbody(no_th))
        self.assertIsNone(self.module.parse_forum_tbody(tbody))


class TestReadThread(unittest.TestCase):
    """验证帖子详情页处理和写盘逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_bds({"request_interval_seconds": 0.2})
        self.redis_client = self.module._fake_redis_client

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_read_thread_writes_file_with_sanitized_title_and_tt_id(self):
        """应提取 tt 编号、清洗文件名并写出 ``.bds`` 文件。"""
        item = {
            "title": "Bad:Name/Part",
            "link": "https://example.com/thread-123-1-1.html",
        }
        response = Mock(text="<html>...tt7654321...</html>")

        with patch.object(self.module, "get_bds_response", return_value=response) as mock_get, patch.object(
            self.module.time, "sleep"
        ) as mock_sleep:
            result = self.module.read_thread(item)

        output_file = Path(self.module.OUTPUT_DIR) / "Bad Name Part[tt7654321].bds"
        self.assertTrue(output_file.exists())
        self.assertEqual(output_file.read_text(encoding="utf-8"), item["link"])
        self.assertEqual(result, item["link"])
        self.assertEqual(self.redis_client.smembers(self.module.REDIS_SEEN_KEY), {item["link"]})
        mock_get.assert_called_once_with("https://example.com/thread-123-1-1.html&_dsign=39e16b34")
        mock_sleep.assert_called_once_with(0.2)

    def test_read_thread_uses_empty_tt_and_logs_warning_when_id_is_missing(self):
        """没有匹配到 tt 编号时，应保留空编号文件名并记录警告。"""
        item = {
            "title": "普通标题",
            "link": "https://example.com/thread-456-1-1.html",
        }
        response = Mock(text="<html>no imdb id</html>")

        with patch.object(self.module, "get_bds_response", return_value=response), patch.object(
            self.module.time, "sleep"
        ), self.assertLogs(self.module.logger.name, level="WARNING") as logs:
            self.module.read_thread(item)

        output_file = Path(self.module.OUTPUT_DIR) / "普通标题[].bds"
        self.assertTrue(output_file.exists())
        self.assertEqual(output_file.read_text(encoding="utf-8"), item["link"])
        self.assertIn("没有找到 tt 编号", logs.output[0])

    def test_build_bds_output_filename_uses_sanitize_filename_result(self):
        """应在替换非法字符后再统一交给 ``sanitize_filename``。"""
        with patch.object(self.module, "sanitize_filename", return_value="safe title") as mock_sanitize:
            result = self.module.build_bds_output_filename("Bad:Name/Part", "tt1234567")

        mock_sanitize.assert_called_once_with("Bad Name Part")
        self.assertEqual(result, "safe title[tt1234567].bds")


class TestProcessAll(unittest.TestCase):
    """验证多线程访问包装逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_bds()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_process_all_collects_successes_and_logs_failures(self):
        """单项失败时应记录错误，其余成功结果照常返回。"""
        items = [
            {"title": "ok-1", "link": "u1"},
            {"title": "bad", "link": "u2"},
            {"title": "ok-2", "link": "u3"},
        ]

        def fake_read_thread(item: dict, redis_client=None):
            if item["title"] == "bad":
                raise RuntimeError("boom")
            return item["title"]

        with patch.object(self.module, "read_thread", side_effect=fake_read_thread) as mock_read, patch.object(
            self.module.logger, "error"
        ) as mock_error:
            result = self.module.process_all(items, max_workers=2)

        self.assertEqual(mock_read.call_count, 3)
        self.assertEqual(sorted(result), ["ok-1", "ok-2"])
        self.assertEqual(mock_error.call_count, 1)
        self.assertIn("boom", mock_error.call_args[0][0])

    def test_process_all_returns_early_for_empty_input(self):
        """空任务列表应直接返回，不去创建 Redis 客户端。"""
        with patch.object(self.module, "get_redis_client") as mock_get_redis:
            result = self.module.process_all([])

        self.assertEqual(result, [])
        mock_get_redis.assert_not_called()


class TestScrapyBdsMain(unittest.TestCase):
    """验证主抓取流程的编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_bds(
            {"group_dict": {"电影": 10, "剧集": 20}, "end_time": "2024-01-01", "thread_number": 8}
        )
        self.redis_client = self.module._fake_redis_client

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_bds_processes_each_group_until_parser_requests_stop(self):
        """应按栏目逐页抓取，并把每个栏目的汇总结果交给 ``process_all``。"""
        movie_page_1 = [{"title": "电影 A", "link": "https://example.com/t/1", "date": "2024-01-03"}]
        movie_page_2 = [{"title": "电影 B", "link": "https://example.com/t/2", "date": "2024-01-02"}]

        with patch.object(
            self.module,
            "parse_forum_page",
            side_effect=[
                (movie_page_1, False),
                (movie_page_2, True),
                ([], False),
            ],
        ) as mock_parse, patch.object(self.module, "process_all", side_effect=[[None, None], []]) as mock_process, patch.object(
            self.module, "get_yesterday_date_str", return_value="2024-01-09"
        ), patch.object(
            self.module, "update_json_config"
        ) as mock_update:
            self.module.scrapy_bds(end_time="2024-01-01")

        self.assertEqual(
            mock_parse.call_args_list,
            [
                call(10, 1, datetime.datetime(2024, 1, 1)),
                call(10, 2, datetime.datetime(2024, 1, 1)),
                call(20, 1, datetime.datetime(2024, 1, 1)),
            ],
        )
        self.assertEqual(
            mock_process.call_args_list,
            [
                call(movie_page_1 + movie_page_2, max_workers=8, redis_client=self.redis_client),
                call([], max_workers=8, redis_client=self.redis_client),
            ],
        )
        mock_update.assert_called_once_with("config/scrapy_bds.json", "end_time", "2024-01-09")

    def test_scrapy_bds_stops_current_group_when_duplicate_item_reappears(self):
        """翻页遇到已收集帖子时，应停止当前栏目并只处理去重后的结果。"""
        first_page = [{"title": "电影 A", "link": "https://example.com/t/1", "date": "2024-01-03"}]
        second_page = [
            {"title": "电影 A (edited)", "link": "https://example.com/t/1", "date": "2024-01-02"},
            {"title": "电影 B", "link": "https://example.com/t/2", "date": "2024-01-02"},
        ]

        with patch.object(
            self.module,
            "parse_forum_page",
            side_effect=[
                (first_page, False),
                (second_page, False),
                ([], False),
            ],
        ) as mock_parse, patch.object(self.module, "process_all", side_effect=[[None], []]) as mock_process, patch.object(
            self.module, "get_yesterday_date_str", return_value="2024-01-09"
        ), patch.object(
            self.module, "update_json_config"
        ) as mock_update:
            self.module.scrapy_bds(end_time="2024-01-01")

        self.assertEqual(
            mock_parse.call_args_list,
            [
                call(10, 1, datetime.datetime(2024, 1, 1)),
                call(10, 2, datetime.datetime(2024, 1, 1)),
                call(20, 1, datetime.datetime(2024, 1, 1)),
            ],
        )
        self.assertEqual(
            mock_process.call_args_list,
            [
                call(first_page, max_workers=8, redis_client=self.redis_client),
                call([], max_workers=8, redis_client=self.redis_client),
            ],
        )
        mock_update.assert_called_once_with("config/scrapy_bds.json", "end_time", "2024-01-09")

    def test_scrapy_bds_resets_start_page_for_each_group(self):
        """每个栏目都应从传入的 ``start_page`` 起扫，而不是串用上个栏目的页码。"""
        page_results = [{"title": "电影 A", "link": "https://example.com/t/1", "date": "2024-01-03"}]

        with patch.object(
            self.module,
            "parse_forum_page",
            side_effect=[
                (page_results, True),
                ([], False),
            ],
        ) as mock_parse, patch.object(self.module, "process_all", side_effect=[[None], []]), patch.object(
            self.module, "get_yesterday_date_str", return_value="2024-01-09"
        ), patch.object(
            self.module, "update_json_config"
        ):
            self.module.scrapy_bds(start_page=3, end_time="2024-01-01")

        self.assertEqual(
            mock_parse.call_args_list,
            [
                call(10, 3, datetime.datetime(2024, 1, 1)),
                call(20, 3, datetime.datetime(2024, 1, 1)),
            ],
        )

    def test_scrapy_bds_reads_end_time_from_config_when_argument_is_omitted(self):
        """未显式传参时，应从配置读取 ``end_time`` 并在成功后自动回写。"""
        page_results = [{"title": "电影 A", "link": "https://example.com/t/1", "date": "2024-01-03"}]

        with patch.object(
            self.module,
            "parse_forum_page",
            side_effect=[
                (page_results, True),
                ([], False),
            ],
        ) as mock_parse, patch.object(self.module, "process_all", side_effect=[[None], []]) as mock_process, patch.object(
            self.module, "get_yesterday_date_str", return_value="2024-01-09"
        ), patch.object(
            self.module, "update_json_config"
        ) as mock_update:
            self.module.scrapy_bds()

        self.assertEqual(mock_parse.call_args_list[0], call(10, 1, datetime.datetime(2024, 1, 1)))
        self.assertEqual(mock_process.call_args_list[0], call(page_results, max_workers=8, redis_client=self.redis_client))
        mock_update.assert_called_once_with("config/scrapy_bds.json", "end_time", "2024-01-09")

    def test_scrapy_bds_skips_end_time_update_when_any_group_has_processing_failures(self):
        """任一栏目详情抓取失败时，不应推进配置里的 ``end_time``。"""
        page_results = [{"title": "电影 A", "link": "https://example.com/t/1", "date": "2024-01-03"}]

        with patch.object(
            self.module,
            "parse_forum_page",
            side_effect=[
                (page_results, True),
                ([], False),
            ],
        ), patch.object(self.module, "process_all", side_effect=[[], []]), patch.object(
            self.module, "get_yesterday_date_str", return_value="2024-01-09"
        ), patch.object(
            self.module, "update_json_config"
        ) as mock_update:
            self.module.scrapy_bds()

        mock_update.assert_not_called()

    def test_scrapy_bds_filters_seen_urls_before_processing_but_still_uses_all_results_for_end_time(self):
        """已存在于 seen 集合的 URL 应跳过详情抓取，但仍参与本轮日期推进。"""
        page_results = [
            {"title": "电影 A", "link": "https://example.com/t/1", "date": "2024-01-03"},
            {"title": "电影 B", "link": "https://example.com/t/2", "date": "2024-01-02"},
        ]
        self.redis_client.sadd(self.module.REDIS_SEEN_KEY, "https://example.com/t/1")

        with patch.object(
            self.module,
            "parse_forum_page",
            side_effect=[
                (page_results, True),
                ([], False),
            ],
        ), patch.object(self.module, "process_all", side_effect=[["https://example.com/t/2"], []]) as mock_process, patch.object(
            self.module, "get_yesterday_date_str", return_value="2024-01-09"
        ), patch.object(
            self.module, "update_json_config"
        ) as mock_update:
            self.module.scrapy_bds()

        self.assertEqual(
            mock_process.call_args_list,
            [
                call([page_results[1]], max_workers=8, redis_client=self.redis_client),
                call([], max_workers=8, redis_client=self.redis_client),
            ],
        )
        mock_update.assert_called_once_with("config/scrapy_bds.json", "end_time", "2024-01-09")


if __name__ == "__main__":
    unittest.main()
