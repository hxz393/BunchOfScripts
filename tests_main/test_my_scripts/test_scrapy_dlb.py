"""
针对 ``my_scripts.scrapy_dlb`` 的单元测试。

这里在隔离环境中加载模块，不依赖真实配置、Redis 或网络请求。
主要验证配置注入、列表扫描入队、详情页写盘、Redis 两段式调度和 ``end_titles`` 回写。
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
from unittest.mock import ANY, Mock, call, patch
try:
    import fakeredis
except ImportError:  # pragma: no cover - 由依赖安装状态决定
    fakeredis = None

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_dlb.py"


class _FallbackFakeRedis:
    """fakeredis 不可用时使用的最小 Redis 替身。"""

    def __init__(self):
        self.values: dict[str, str] = {}
        self.lists: dict[str, list[str]] = {}
        self.sets: dict[str, set[str]] = {}

    def get(self, key: str):
        return self.values.get(key)

    def set(self, key: str, value: str) -> None:
        self.values[key] = value

    def delete(self, *keys: str) -> None:
        for key in keys:
            self.values.pop(key, None)
            self.lists.pop(key, None)

    def llen(self, key: str) -> int:
        return len(self.lists.get(key, []))

    def rpush(self, key: str, *values: str) -> int:
        self.lists.setdefault(key, []).extend(values)
        return len(self.lists[key])

    def rpoplpush(self, source: str, destination: str):
        source_list = self.lists.get(source, [])
        if not source_list:
            return None
        value = source_list.pop()
        self.lists.setdefault(destination, []).insert(0, value)
        return value

    def sadd(self, key: str, value: str) -> int:
        value_set = self.sets.setdefault(key, set())
        if value in value_set:
            return 0
        value_set.add(value)
        return 1

    def smembers(self, key: str):
        return self.sets.get(key, set()).copy()


if fakeredis is None:
    class FakeRedis(_FallbackFakeRedis):
        """回退到手写内存 Redis。"""

else:
    class FakeRedis(fakeredis.FakeRedis):
        """优先使用带真实命令语义的 fakeredis。"""

        def __init__(self, *args, **kwargs):
            super().__init__(decode_responses=True)


def fake_serialize_payload(payload: dict) -> str:
    """与真实 helper 保持一致的 JSON 序列化。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def fake_deserialize_payload(payload: str) -> dict:
    """与真实 helper 保持一致的 JSON 反序列化。"""
    return json.loads(payload)


def fake_push_items_to_queue(
        redis_client,
        items: list[dict],
        *,
        seen_key: str,
        pending_key: str,
        unique_value,
        serializer,
) -> int:
    """最小入队实现，使用 set 去重后写入列表。"""
    enqueued_count = 0
    for item in items:
        if redis_client.sadd(seen_key, unique_value(item)):
            redis_client.rpush(pending_key, serializer(item))
            enqueued_count += 1
    return enqueued_count


def load_scrapy_dlb(config: dict | None = None):
    """在最小依赖环境中加载 ``scrapy_dlb`` 模块。"""
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "dlb_url": "https://example.com",
        "output_dir": str(Path(temp_dir.name) / "downloads"),
        "dlb_cookie": "cookie=value",
        "request_head": {"User-Agent": "unit-test"},
        "end_titles": ["Old Movie – 1.0 GB", "Older Movie – 0.9 GB"],
        "thread_number": 30,
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
    fake_my_module.update_json_config = lambda _path, _key, _value: None

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    fake_scrapy_redis = types.ModuleType("scrapy_redis")
    fake_scrapy_redis.serialize_payload = fake_serialize_payload
    fake_scrapy_redis.deserialize_payload = fake_deserialize_payload
    fake_scrapy_redis.push_items_to_queue = fake_push_items_to_queue
    fake_scrapy_redis.get_redis_client = lambda: FakeRedis()
    fake_scrapy_redis.drain_queue = lambda *args, **kwargs: {"processed": 0, "success": 0, "failed": 0}

    fake_redis = types.ModuleType("redis")
    fake_redis.Redis = FakeRedis

    spec = importlib.util.spec_from_file_location(
        f"scrapy_dlb_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "retrying": fake_retrying,
            "scrapy_redis": fake_scrapy_redis,
            "redis": fake_redis,
        },
    ):
        spec.loader.exec_module(module)

    return module, temp_dir


def build_list_page(*items: str) -> str:
    """构造一个最小可用的列表页 HTML。"""
    return "<html><body>" + "".join(items) + "</body></html>"


def build_movie_block(title: str, href: str, size: str) -> str:
    """构造一个最小可用的列表页条目。"""
    return f"""
    <div class="movies_block">
      <div class="movie_title_list">
        <a href="{href}">
          <span class="movie_title_list_text">{title}</span>
        </a>
      </div>
      <div class="type_banner_size">{size}</div>
    </div>
    """


def build_detail_page(*hrefs: str) -> str:
    """构造一个最小可用的详情页 HTML。"""
    return "<html><body>" + "".join(f'<a href="{href}">link</a>' for href in hrefs) + "</body></html>"


class TestModuleLoad(unittest.TestCase):
    """验证模块导入时的配置注入。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dlb()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_scrapy_dlb_uses_injected_config(self):
        """模块加载后应暴露注入的站点地址、输出目录和配置化默认值。"""
        self.assertEqual(self.module.DLB_URL, "https://example.com")
        self.assertTrue(str(self.module.OUTPUT_DIR).endswith("downloads"))
        self.assertEqual(self.module.THREAD_NUMBER, 30)
        self.assertEqual(self.module.REQUEST_HEAD["User-Agent"], "unit-test")
        self.assertEqual(self.module.REQUEST_HEAD["Cookie"], "cookie=value")


class TestDlbHelpers(unittest.TestCase):
    """验证主流程辅助函数。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dlb()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_build_dlb_page_url_formats_archive_page_address(self):
        """分页 URL 应基于配置的站点前缀拼接。"""
        self.assertEqual(
            self.module.build_dlb_page_url(3),
            "https://example.com/cat/movie/page/3/",
        )

    def test_should_stop_scrapy_returns_true_only_when_end_title_is_present(self):
        """任一截止标题存在时应停止翻页，否则继续。"""
        result_list = [
            {"title": "New Movie – 2.0 GB", "link": "https://example.com/new", "size": "2.0GB"},
            {"title": "Old Movie – 1.0 GB", "link": "https://example.com/old", "size": "1.0GB"},
        ]

        self.assertTrue(self.module.should_stop_scrapy(result_list, ["Old Movie – 1.0 GB", "Older Movie – 0.9 GB"]))
        self.assertFalse(self.module.should_stop_scrapy(result_list, ["Missing Movie", "Another Missing Movie"]))

    def test_select_next_end_titles_returns_first_two_titles_from_first_page(self):
        """下一轮截止标题应取首次访问页最前面的两个标题。"""
        result_list = [
            {"title": "Newest Movie – 2.0 GB", "link": "https://example.com/newest", "size": "2.0GB"},
            {"title": "Second Movie – 1.8 GB", "link": "https://example.com/second", "size": "1.8GB"},
            {"title": "Third Movie – 1.0 GB", "link": "https://example.com/third", "size": "1.0GB"},
        ]

        self.assertEqual(
            self.module.select_next_end_titles(result_list),
            ["Newest Movie – 2.0 GB", "Second Movie – 1.8 GB"],
        )

    def test_get_current_end_titles_uses_configured_titles(self):
        """当前截止标题列表应来自配置。"""
        self.assertEqual(
            self.module.get_current_end_titles(),
            ["Old Movie – 1.0 GB", "Older Movie – 0.9 GB"],
        )


class TestGetDlbResponse(unittest.TestCase):
    """验证单次请求逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dlb()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_dlb_response_returns_response_when_status_and_body_are_valid(self):
        """状态码为 200 且正文足够长时应直接返回响应对象。"""
        response = Mock(status_code=200, text="x" * 10001)

        with patch.object(self.module.session, "get", return_value=response) as mock_get:
            result = self.module.get_dlb_response("https://example.com/post")

        self.assertIs(result, response)
        mock_get.assert_called_once_with(
            "https://example.com/post",
            timeout=30,
            verify=False,
            headers=self.module.REQUEST_HEAD,
        )

    def test_get_dlb_response_raises_when_status_code_is_not_200(self):
        """请求返回非 200 状态码时应抛出异常。"""
        response = Mock(status_code=503, text="x" * 10001)

        with patch.object(self.module.session, "get", return_value=response):
            with self.assertRaisesRegex(Exception, "请求失败"):
                self.module.get_dlb_response("https://example.com/post")

    def test_get_dlb_response_raises_when_body_is_too_short(self):
        """正文过短时应判定为被封锁并抛出异常。"""
        response = Mock(status_code=200, text="blocked")

        with patch.object(self.module.session, "get", return_value=response):
            with self.assertRaisesRegex(Exception, "请求被封锁"):
                self.module.get_dlb_response("https://example.com/post")


class TestParseDlbResponse(unittest.TestCase):
    """验证列表页解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dlb()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_dlb_response_extracts_title_link_and_size(self):
        """应从列表页提取标题、详情链接和去空格后的体积。"""
        response = Mock(
            text=build_list_page(
                build_movie_block(
                    title="Movie One",
                    href="/post-1",
                    size="22.4 GB",
                ),
                build_movie_block(
                    title="Movie Two",
                    href="/post-2",
                    size="700 MB",
                ),
            )
        )

        result = self.module.parse_dlb_response(response)

        self.assertEqual(
            result,
            [
                {
                    "title": "Movie One",
                    "link": "https://example.com/post-1",
                    "size": "22.4GB",
                },
                {
                    "title": "Movie Two",
                    "link": "https://example.com/post-2",
                    "size": "700MB",
                },
            ],
        )


class TestVisitDlbUrl(unittest.TestCase):
    """验证详情页访问和写盘逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dlb()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_visit_dlb_url_extracts_imdb_and_writes_release_file(self):
        """详情页包含 IMDb 链接时，应提取 ID 并按规则落盘。"""
        result_item = {
            "title": "Movie / Title: 2026",
            "link": "https://example.com/post",
            "size": "22.4GB",
        }
        detail_session = Mock()
        response = Mock(text=build_detail_page("https://www.imdb.com/title/tt1234567/"))

        with patch.object(self.module, "build_dlb_session", return_value=detail_session) as mock_session, patch.object(
            self.module,
            "get_dlb_response",
            return_value=response,
        ) as mock_get, patch.object(
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
            self.module.visit_dlb_url(result_item)

        mock_session.assert_called_once_with()
        mock_get.assert_called_once_with("https://example.com/post", request_session=detail_session)
        mock_normalize.assert_called_once_with("Movie / Title: 2026")
        mock_sanitize.assert_called_once_with("Normalized / Title: 2026")
        mock_write.assert_called_once_with(
            str(Path(self.module.OUTPUT_DIR) / "Sanitized Title 2026 (22.4GB)[tt1234567].dlb"),
            ["https://example.com/post"],
        )

    def test_visit_dlb_url_falls_back_to_loose_tt_match_when_imdb_link_is_noncanonical(self):
        """详情页没有标准 IMDb URL 时，仍应从其它链接中回退提取 ``tt`` 编号。"""
        result_item = {
            "title": "Fallback Title",
            "link": "https://example.com/post",
            "size": "1.0GB",
        }
        response = Mock(text=build_detail_page("https://example.com/redirect?target=tt7654321"))

        with patch.object(self.module, "build_dlb_session", return_value=Mock()), patch.object(
            self.module,
            "get_dlb_response",
            return_value=response,
        ), patch.object(
            self.module,
            "write_list_to_file",
            return_value=True,
        ) as mock_write:
            self.module.visit_dlb_url(result_item)

        mock_write.assert_called_once_with(
            str(Path(self.module.OUTPUT_DIR) / "Fallback Title (1.0GB)[tt7654321].dlb"),
            ["https://example.com/post"],
        )

    def test_extract_dlb_imdb_id_from_links_prefers_canonical_imdb_url(self):
        """同时存在多种链接时，应优先取标准 IMDb 标题页。"""
        imdb_id = self.module.extract_dlb_imdb_id_from_links(
            [
                "https://example.com/redirect?target=tt7654321",
                "https://www.imdb.com/title/tt1234567/",
            ]
        )

        self.assertEqual(imdb_id, "tt1234567")

    def test_build_dlb_output_filename_uses_sanitized_title_size_and_imdb(self):
        """输出文件名应基于标题、体积和 IMDb 编号拼接。"""
        result_item = {
            "title": "Movie / Title: 2026",
            "size": "22.4GB",
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
            file_name = self.module.build_dlb_output_filename(result_item, "tt1234567")

        self.assertEqual(file_name, "Sanitized Title 2026 (22.4GB)[tt1234567].dlb")
        mock_normalize.assert_called_once_with("Movie / Title: 2026")
        mock_sanitize.assert_called_once_with("Normalized / Title: 2026")


class TestEnqueueDlbPosts(unittest.TestCase):
    """验证列表扫描入队和断点逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dlb()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_enqueue_dlb_posts_stops_when_end_title_is_found_on_first_page(self):
        """第一页命中截止标题时应完成扫描、写入下一轮标题并把条目入队。"""
        response = Mock()
        result_list = [
            {"title": "Old Movie – 1.0 GB", "link": "https://example.com/post-1", "size": "1.0GB"},
            {"title": "Older Movie – 0.9 GB", "link": "https://example.com/post-2", "size": "0.9GB"},
        ]

        with patch.object(self.module, "get_dlb_response", return_value=response) as mock_get, patch.object(
            self.module,
            "parse_dlb_response",
            return_value=result_list,
        ) as mock_parse:
            self.module.enqueue_dlb_posts(start_page=3, redis_client=self.redis_client)

        mock_get.assert_called_once_with("https://example.com/cat/movie/page/3/")
        mock_parse.assert_called_once_with(response)
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY), "1")
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_PAGE_KEY), "4")
        self.assertEqual(
            self.module.deserialize_payload(self.redis_client.get(self.module.REDIS_NEXT_END_TITLES_KEY))["titles"],
            ["Old Movie – 1.0 GB", "Older Movie – 0.9 GB"],
        )
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 2)
        self.assertEqual(
            self.module.deserialize_payload(self.redis_client.lrange(self.module.REDIS_PENDING_KEY, 0, 0)[0]),
            {"link": "https://example.com/post-1", "size": "1.0GB", "title": "Old Movie – 1.0 GB"},
        )

    def test_enqueue_dlb_posts_moves_to_next_page_until_end_title_is_found(self):
        """未命中截止标题时应继续抓取下一页，且下一轮标题只记录第一页。"""
        first_response = Mock()
        second_response = Mock()
        first_result_list = [
            {"title": "New Movie – 2.0 GB", "link": "https://example.com/new", "size": "2.0GB"},
            {"title": "Second Movie – 1.8 GB", "link": "https://example.com/second", "size": "1.8GB"},
        ]
        second_result_list = [{"title": "Old Movie – 1.0 GB", "link": "https://example.com/old", "size": "1.0GB"}]

        with patch.object(
            self.module,
            "get_dlb_response",
            side_effect=[first_response, second_response],
        ) as mock_get, patch.object(
            self.module,
            "parse_dlb_response",
            side_effect=[first_result_list, second_result_list],
        ) as mock_parse:
            self.module.enqueue_dlb_posts(start_page=3, redis_client=self.redis_client)

        self.assertEqual(
            mock_get.call_args_list,
            [
                call("https://example.com/cat/movie/page/3/"),
                call("https://example.com/cat/movie/page/4/"),
            ],
        )
        self.assertEqual(mock_parse.call_args_list, [call(first_response), call(second_response)])
        self.assertEqual(
            self.module.deserialize_payload(self.redis_client.get(self.module.REDIS_NEXT_END_TITLES_KEY))["titles"],
            ["New Movie – 2.0 GB", "Second Movie – 1.8 GB"],
        )
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY), "1")
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_PAGE_KEY), "5")
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 3)


class TestRecoverAndDrainDlbQueue(unittest.TestCase):
    """验证 Redis 队列恢复与消费编排。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dlb()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_recover_dlb_processing_when_pending_is_empty_moves_payloads_back(self):
        """待处理为空且处理中有残留时，应回退到待处理队列。"""
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, "task-1", "task-2")

        recovered = self.module.recover_dlb_processing_when_pending_is_empty(self.redis_client)

        self.assertEqual(recovered, 2)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PROCESSING_KEY), 0)
        self.assertEqual(self.redis_client.lrange(self.module.REDIS_PENDING_KEY, 0, -1), ["task-1", "task-2"])

    def test_drain_dlb_queue_delegates_to_shared_drain_queue_helper(self):
        """消费阶段应把队列参数和 worker 函数交给共享 helper。"""
        with patch.object(self.module, "drain_queue") as mock_drain:
            self.module.drain_dlb_queue(redis_client=self.redis_client)

        mock_drain.assert_called_once_with(
            self.redis_client,
            pending_key=self.module.REDIS_PENDING_KEY,
            processing_key=self.module.REDIS_PROCESSING_KEY,
            max_workers=self.module.THREAD_NUMBER,
            worker=self.module.visit_dlb_url,
            deserialize=self.module.deserialize_payload,
            logger=self.module.logger,
            queue_label="DLB",
            identify_item=ANY,
            recover_processing_on_start=False,
            keep_failed_in_processing=True,
        )


class TestFinalizeDlbRun(unittest.TestCase):
    """验证回写截止标题和清理状态的逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dlb()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_finalize_dlb_run_updates_end_titles_after_scan_and_queue_finish(self):
        """列表扫描完成且队列清空后，应回写新的截止标题并清理运行状态。"""
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")
        self.redis_client.set(
            self.module.REDIS_NEXT_END_TITLES_KEY,
            self.module.serialize_payload({"titles": ["Newest Movie", "Second Movie"]}),
        )
        self.redis_client.set(self.module.REDIS_SCAN_PAGE_KEY, "8")

        with patch.object(self.module, "update_json_config") as mock_update:
            self.module.finalize_dlb_run(redis_client=self.redis_client)

        mock_update.assert_called_once_with(
            self.module.CONFIG_PATH,
            "end_titles",
            ["Newest Movie", "Second Movie"],
        )
        self.assertIsNone(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY))
        self.assertIsNone(self.redis_client.get(self.module.REDIS_SCAN_PAGE_KEY))
        self.assertIsNone(self.redis_client.get(self.module.REDIS_NEXT_END_TITLES_KEY))

    def test_finalize_dlb_run_skips_update_when_processing_queue_is_not_empty(self):
        """处理中仍有残留时，不应回写截止标题。"""
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")
        self.redis_client.set(
            self.module.REDIS_NEXT_END_TITLES_KEY,
            self.module.serialize_payload({"titles": ["Newest Movie", "Second Movie"]}),
        )
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, "task-1")

        with patch.object(self.module, "update_json_config") as mock_update:
            self.module.finalize_dlb_run(redis_client=self.redis_client)

        mock_update.assert_not_called()
        self.assertIsNotNone(self.redis_client.get(self.module.REDIS_NEXT_END_TITLES_KEY))


class TestScrapyDlbMain(unittest.TestCase):
    """验证主入口调度逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dlb()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_dlb_runs_recover_enqueue_drain_and_finalize_in_order(self):
        """主入口应先恢复，再入队、消费，并在 finally 中执行收尾。"""
        with patch.object(self.module, "get_redis_client", return_value=self.redis_client) as mock_get_redis, patch.object(
            self.module,
            "recover_dlb_processing_when_pending_is_empty",
        ) as mock_recover, patch.object(
            self.module,
            "enqueue_dlb_posts",
        ) as mock_enqueue, patch.object(
            self.module,
            "drain_dlb_queue",
        ) as mock_drain, patch.object(
            self.module,
            "finalize_dlb_run",
        ) as mock_finalize:
            self.module.scrapy_dlb(start_page=5)

        mock_get_redis.assert_called_once_with()
        mock_recover.assert_called_once_with(self.redis_client)
        mock_enqueue.assert_called_once_with(start_page=5, redis_client=self.redis_client)
        mock_drain.assert_called_once_with(redis_client=self.redis_client)
        mock_finalize.assert_called_once_with(redis_client=self.redis_client)

    def test_scrapy_dlb_still_finalizes_when_drain_queue_raises(self):
        """消费阶段异常时，收尾逻辑仍应执行。"""
        with patch.object(self.module, "get_redis_client", return_value=self.redis_client), patch.object(
            self.module,
            "recover_dlb_processing_when_pending_is_empty",
        ), patch.object(
            self.module,
            "enqueue_dlb_posts",
        ), patch.object(
            self.module,
            "drain_dlb_queue",
            side_effect=RuntimeError("boom"),
        ), patch.object(
            self.module,
            "finalize_dlb_run",
        ) as mock_finalize:
            with self.assertRaisesRegex(RuntimeError, "boom"):
                self.module.scrapy_dlb(start_page=5)

        mock_finalize.assert_called_once_with(redis_client=self.redis_client)


if __name__ == "__main__":
    unittest.main()
