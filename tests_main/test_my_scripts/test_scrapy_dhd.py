"""
针对 ``my_scripts.scrapy_dhd`` 的单元测试。

这里不依赖真实配置文件，也不会发出真实网络请求。
目标是只验证抓取脚本的核心行为：
1. 页面解析、文件名规范化和磁链生成。
2. 单条详情抓取、分页入队、队列消费和 newest_id 回写。
3. ``.dhd`` 转 ``.log`` 的本地 I/O 编排逻辑。
"""

import copy
import hashlib
import importlib.util
import sys
import tempfile
import types
import unittest
import urllib.parse
import uuid
from pathlib import Path
from unittest.mock import Mock, call, patch

import requests

requests.packages.urllib3.disable_warnings()

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_dhd.py"


def fake_bencode_encode(value):
    """最小实现：支持 ``bytes`` / ``str`` / ``int`` / ``list`` / ``dict``。"""
    if isinstance(value, dict):
        encoded_items = []
        for key in sorted(value):
            encoded_items.append(fake_bencode_encode(key))
            encoded_items.append(fake_bencode_encode(value[key]))
        return b"d" + b"".join(encoded_items) + b"e"
    if isinstance(value, list):
        return b"l" + b"".join(fake_bencode_encode(item) for item in value) + b"e"
    if isinstance(value, int):
        return f"i{value}e".encode("ascii")
    if isinstance(value, str):
        value = value.encode("utf-8")
    if isinstance(value, bytes):
        return str(len(value)).encode("ascii") + b":" + value
    raise TypeError(f"Unsupported type for fake bencode: {type(value)!r}")


def fake_bencode_decode(data: bytes):
    """最小实现：只解析本测试会生成的 bencode 结构。"""

    def parse(index: int):
        token = data[index:index + 1]
        if token == b"i":
            end = data.index(b"e", index)
            return int(data[index + 1:end]), end + 1
        if token == b"l":
            index += 1
            items = []
            while data[index:index + 1] != b"e":
                item, index = parse(index)
                items.append(item)
            return items, index + 1
        if token == b"d":
            index += 1
            mapping = {}
            while data[index:index + 1] != b"e":
                key, index = parse(index)
                value, index = parse(index)
                mapping[key] = value
            return mapping, index + 1
        if token.isdigit():
            colon = data.index(b":", index)
            length = int(data[index:colon])
            start = colon + 1
            end = start + length
            return data[start:end], end
        raise ValueError(f"Invalid bencode token at {index}: {token!r}")

    result, next_index = parse(0)
    if next_index != len(data):
        raise ValueError("Trailing bytes after fake bencode decode")
    return result


def load_scrapy_dhd(config: dict | None = None):
    """
    在隔离环境中加载 ``scrapy_dhd`` 模块。

    被测模块在 import 时就会读取配置并导入 ``my_module`` / ``retrying`` /
    ``bencodepy``，所以这里先注入假的依赖，避免测试依赖本地真实环境。
    """
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "dhd_url": "https://example.com",
        "dhd_movie_url": "https://example.com/movie/",
        "dhd_dl_url": "https://example.com/download/",
        "newest_id": 100,
        "dhd_cookie": "cookie=value",
        "request_head": {"User-Agent": "unit-test"},
        "output_dir": str(Path(temp_dir.name) / "downloads"),
        "thread_number": 3,
        "redis_host": "127.0.0.1",
        "redis_port": 6379,
        "redis_db": 0,
        "redis_pending_key": "dhd_pending",
        "redis_processing_key": "dhd_processing",
        "redis_failed_key": "dhd_failed",
        "redis_seen_key": "dhd_seen",
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
    fake_my_module.read_file_to_list = lambda path: Path(path).read_text(encoding="utf-8").splitlines()

    fake_retrying = types.ModuleType("retrying")

    def fake_retry(*args, **kwargs):
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

    fake_retrying.retry = fake_retry

    fake_redis = types.ModuleType("redis")

    class DummyRedis:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    fake_redis.Redis = DummyRedis

    fake_bencodepy = types.ModuleType("bencodepy")
    fake_bencodepy.encode = fake_bencode_encode
    fake_bencodepy.decode = fake_bencode_decode

    spec = importlib.util.spec_from_file_location(
        f"scrapy_dhd_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "retrying": fake_retrying,
            "redis": fake_redis,
            "bencodepy": fake_bencodepy,
        },
    ):
        spec.loader.exec_module(module)

    return module, temp_dir


def build_topic_html(
        topic_id: str = "123",
        title: str = "Movie Title",
        include_title_wrapper: bool = True,
        include_anchor: bool = True,
        href: str | None = None,
        extra_classes: str = "",
) -> str:
    """构造一条最小可用的 DHD 帖子 HTML。"""
    if not include_title_wrapper:
        inner_html = ""
    elif include_anchor:
        if href is None:
            href = f"{topic_id}_11.html"
        href_attr = f' href="{href}"' if href is not None else ""
        inner_html = f'<div class="title media-heading"><a{href_attr}>{title}</a></div>'
    else:
        inner_html = '<div class="title media-heading"></div>'

    extra_class_attr = f" {extra_classes.strip()}" if extra_classes.strip() else ""
    return f'<div class="topic media topic-visited{extra_class_attr}">{inner_html}</div>'


def build_page_html(*topics: str) -> str:
    """把若干帖子块拼成最小可用页面。"""
    return f"<html><body>{''.join(topics)}</body></html>"


class FakeRedisPipeline:
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


class FakeRedis:
    """用于测试 DHD Redis 队列流程的内存实现。"""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.sets = {}
        self.lists = {}

    def pipeline(self):
        return FakeRedisPipeline(self)

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


class TestModuleLoad(unittest.TestCase):
    """验证模块导入时的配置注入。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dhd()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_scrapy_dhd_injects_cookie_into_request_head(self):
        """模块加载时应把配置里的 cookie 注入请求头。"""
        self.assertEqual(self.module.REQUEST_HEAD["Cookie"], "cookie=value")


class TestGetDhdResponse(unittest.TestCase):
    """验证同步单页请求逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dhd()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_dhd_response_returns_text_when_request_succeeds(self):
        """请求成功时应返回按 GBK 解码后的文本。"""
        response = Mock(status_code=200, text="页面内容")

        with patch.object(self.module.requests, "get", return_value=response) as mock_get:
            result = self.module.get_dhd_response("https://example.com/topic/1")

        self.assertEqual(result, "页面内容")
        self.assertEqual(response.encoding, "gbk")
        mock_get.assert_called_once_with(
            "https://example.com/topic/1",
            headers=self.module.REQUEST_HEAD,
            timeout=15,
            verify=False,
        )

    def test_get_dhd_response_retries_and_raises_after_retries_exhausted(self):
        """状态码持续异常时，应按重试次数耗尽后抛错。"""
        bad_response = Mock(status_code=503, text="")

        with patch.object(self.module.requests, "get", return_value=bad_response) as mock_get:
            with self.assertRaisesRegex(Exception, "请求失败，状态码：503"):
                self.module.get_dhd_response("https://example.com/topic/1")

        self.assertEqual(mock_get.call_count, 15)


class TestParseDhdResponse(unittest.TestCase):
    """验证 DHD 列表页解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dhd()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_dhd_response_returns_structured_items(self):
        """结构完整的帖子块应被解析成名称、链接和 ID。"""
        html = build_page_html(build_topic_html(topic_id="123", title="电影标题"))

        result = self.module.parse_dhd_response(html)

        self.assertEqual(
            result,
            [
                {
                    "name": "电影标题",
                    "url": "https://example.com/123_11.html",
                    "id": "123",
                }
            ],
        )

    def test_parse_dhd_response_logs_and_skips_invalid_topics(self):
        """缺少标题容器或标题链接时，应记录日志并跳过无效项。"""
        html = build_page_html(
            build_topic_html(topic_id="200", title="正常帖子"),
            build_topic_html(topic_id="201", include_title_wrapper=False),
            build_topic_html(topic_id="202", include_anchor=False),
        )

        with self.assertLogs(self.module.logger.name, level="WARNING") as logs:
            result = self.module.parse_dhd_response(html)

        self.assertEqual(
            result,
            [
                {
                    "name": "正常帖子",
                    "url": "https://example.com/200_11.html",
                    "id": "200",
                }
            ],
        )
        self.assertIn("没有找到 title media-heading", logs.output[0])
        self.assertIn("没有找到 title media-heading 中的 a 标签", logs.output[1])

    def test_parse_dhd_response_accepts_topic_with_extra_classes(self):
        """额外 class 不应影响帖子块的匹配。"""
        html = build_page_html(
            build_topic_html(topic_id="300", title="带额外类名", extra_classes="pinned highlight"),
        )

        result = self.module.parse_dhd_response(html)

        self.assertEqual(
            result,
            [
                {
                    "name": "带额外类名",
                    "url": "https://example.com/300_11.html",
                    "id": "300",
                }
            ],
        )

    def test_parse_dhd_response_logs_and_skips_topic_when_href_is_missing(self):
        """标题链接缺少 href 时，应跳过当前帖子而不是中断整页解析。"""
        html = build_page_html(
            build_topic_html(topic_id="400", title="正常帖子"),
            build_topic_html(topic_id="401", title="坏帖子", href=None),
        )
        html = html.replace(' href="401_11.html"', "")

        with self.assertLogs(self.module.logger.name, level="WARNING") as logs:
            result = self.module.parse_dhd_response(html)

        self.assertEqual(
            result,
            [
                {
                    "name": "正常帖子",
                    "url": "https://example.com/400_11.html",
                    "id": "400",
                }
            ],
        )
        self.assertIn("a 标签缺少 href", logs.output[0])


class TestRenameAndExtractHelpers(unittest.TestCase):
    """验证文件名整理和 HTML 提取辅助函数。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dhd()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_rename_file_reformats_valid_release_name(self):
        """符合规则的文件名应被重新排布为英文名 + 中文别名。"""
        result = self.module.rename_file("中文名.Original.Title.2024.1080p.BluRay[tt1234567].dhd")

        self.assertEqual(result, "Original Title 2024.1080p.BluRay [中文名][tt1234567].dhd")

    def test_rename_file_returns_original_when_chinese_part_contains_letters(self):
        """中文段里出现英文字母时，应保持原文件名不变。"""
        file_name = "中文A.Original.Title.2024.1080p.BluRay[tt1234567].dhd"

        self.assertEqual(self.module.rename_file(file_name), file_name)

    def test_rename_file_returns_original_when_original_part_contains_chinese(self):
        """原名段里出现中文时，应保持原文件名不变。"""
        file_name = "中文名.原版.Title.2024.1080p.BluRay[tt1234567].dhd"

        self.assertEqual(self.module.rename_file(file_name), file_name)

    def test_extract_dl_url_uses_torrent_icon_link_first(self):
        """优先使用带 torrent 图标的主下载链接。"""
        html = """
        <span style="white-space: nowrap">
          <a href="download.php?id=1">
            <img src="static/image/filetype/torrent.gif" />
          </a>
        </span>
        """

        result = self.module.extract_dl_url(html)

        self.assertEqual(result, "https://example.com/download.php?id=1")

    def test_extract_dl_url_falls_back_to_attnm_link(self):
        """主下载链接缺失时，应回退到 ``p.attnm`` 里的附件链接。"""
        html = '<p class="attnm"><a href="download.php?id=2">torrent</a></p>'

        result = self.module.extract_dl_url(html)

        self.assertEqual(result, "https://example.com/download.php?id=2")

    def test_extract_imdb_id_returns_match_and_empty_string_when_missing(self):
        """应能提取 IMDb 编号；没有时返回空字符串。"""
        self.assertEqual(self.module.extract_imdb_id("https://www.imdb.com/title/tt7654321/"), "tt7654321")
        self.assertEqual(self.module.extract_imdb_id("no imdb link here"), "")


class TestGetDhdAndTorrent(unittest.TestCase):
    """验证同步请求和种子写盘逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dhd()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_dhd_returns_response_and_sets_gbk_encoding(self):
        """请求成功时应返回响应对象，并统一设置 GBK 编码。"""
        response = Mock(status_code=200)
        session = Mock()
        session.get.return_value = response

        result = self.module.get_dhd(session, "https://example.com/topic/1")

        self.assertIs(result, response)
        self.assertEqual(response.encoding, "gbk")
        session.get.assert_called_once_with(
            "https://example.com/topic/1",
            timeout=15,
            verify=False,
            headers=self.module.REQUEST_HEAD,
        )

    def test_get_dhd_raises_when_status_code_is_not_200(self):
        """请求返回非 200 状态码时应抛出异常。"""
        response = Mock(status_code=503)
        session = Mock()
        session.get.return_value = response

        with self.assertRaisesRegex(Exception, "503"):
            self.module.get_dhd(session, "https://example.com/topic/1")

    def test_get_dhd_torrent_writes_binary_file(self):
        """下载成功时应把种子内容按二进制写到目标路径。"""
        response = Mock(status_code=200, content=b"torrent-bytes")
        session = Mock()
        session.get.return_value = response
        torrent_path = Path(self.temp_dir.name) / "movie.torrent"

        self.module.get_dhd_torrent(session, "https://example.com/download.php?id=1", str(torrent_path))

        self.assertEqual(torrent_path.read_bytes(), b"torrent-bytes")
        session.get.assert_called_once_with(
            "https://example.com/download.php?id=1",
            timeout=25,
            verify=False,
            headers=self.module.REQUEST_HEAD,
        )


class TestTorrentToMagnet(unittest.TestCase):
    """验证 torrent 文件到磁链的转换逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dhd()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_torrent_to_magnet_keeps_all_trackers_from_announce_list(self):
        """存在 ``announce-list`` 时，应保留全部 tracker 并带上显示名。"""
        torrent_dict = {
            b"announce-list": [
                [b"https://tracker.example/announce"],
                [b"https://backup.example/announce"],
            ],
            b"info": {
                b"length": 123,
                b"name": "Example Name".encode("utf-8"),
                b"piece length": 16384,
                b"pieces": b"12345678901234567890",
            },
        }
        torrent_path = Path(self.temp_dir.name) / "movie.torrent"
        torrent_path.write_bytes(fake_bencode_encode(torrent_dict))
        info_hash = hashlib.sha1(fake_bencode_encode(torrent_dict[b"info"])).hexdigest()

        result = self.module.torrent_to_magnet(str(torrent_path))

        expected = (
            f"magnet:?xt=urn:btih:{info_hash}"
            f"&dn={urllib.parse.quote('Example Name')}"
            f"&tr={urllib.parse.quote('https://tracker.example/announce')}"
            f"&tr={urllib.parse.quote('https://backup.example/announce')}"
        )
        self.assertEqual(result, expected)

    def test_torrent_to_magnet_falls_back_to_announce_when_list_is_missing(self):
        """不存在 ``announce-list`` 时，应回退到 ``announce`` 字段。"""
        torrent_dict = {
            b"announce": b"https://single-tracker.example/announce",
            b"info": {
                b"length": 456,
                b"name": "Latin Name".encode("utf-8"),
                b"piece length": 16384,
                b"pieces": b"abcdefghijabcdefghij",
            },
        }
        torrent_path = Path(self.temp_dir.name) / "movie.torrent"
        torrent_path.write_bytes(fake_bencode_encode(torrent_dict))
        info_hash = hashlib.sha1(fake_bencode_encode(torrent_dict[b"info"])).hexdigest()

        result = self.module.torrent_to_magnet(str(torrent_path))

        expected = (
            f"magnet:?xt=urn:btih:{info_hash}"
            f"&dn={urllib.parse.quote('Latin Name')}"
            f"&tr={urllib.parse.quote('https://single-tracker.example/announce')}"
        )
        self.assertEqual(result, expected)


class TestWorkingDhd(unittest.TestCase):
    """验证单条详情抓取和落盘逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dhd()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_working_dhd_writes_expected_file(self):
        """应写出经过重命名后的 ``.dhd`` 文件，并保存发布页和下载页链接。"""
        info = {
            "name": "中文名.Original.Title.2024.1080p.BluRay",
            "url": "https://example.com/topic/123",
        }

        with patch.object(self.module, "get_dhd_response", return_value="<html></html>"), patch.object(
            self.module, "extract_imdb_id", return_value="tt1234567"
        ), patch.object(
            self.module, "extract_dl_url", return_value="https://example.com/download.php?id=123"
        ):
            self.module.working_dhd(info)

        output_path = Path(self.module.OUTPUT_DIR) / "Original Title 2024.1080p.BluRay [中文名][tt1234567].dhd"
        self.assertTrue(output_path.exists())
        self.assertEqual(
            output_path.read_text(encoding="utf-8"),
            "https://example.com/topic/123\nhttps://example.com/download.php?id=123",
        )


class TestScrapyDhd(unittest.TestCase):
    """验证 Redis 两阶段抓取流程的编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dhd({"newest_id": 100})
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_dhd_calls_enqueue_and_drain(self):
        """入口函数应先入队，再消费 Redis 队列。"""
        with patch.object(self.module, "enqueue_dhd_posts") as mock_enqueue, patch.object(
            self.module, "drain_dhd_queue"
        ) as mock_drain:
            self.module.scrapy_dhd(start_page=3)

        mock_enqueue.assert_called_once_with(start_page=3)
        mock_drain.assert_called_once_with()

    def test_fetch_dhd_batch_raises_when_batch_keeps_returning_no_results(self):
        """列表页连续返回空结果时，应在重试耗尽后抛出异常。"""
        with patch.object(
            self.module, "get_dhd_response", return_value=""
        ) as mock_get, patch.object(
            self.module.time, "sleep"
        ) as mock_sleep:
            with self.assertRaisesRegex(Exception, "批量请求失败"):
                self.module.fetch_dhd_batch(start_page=1)

        self.assertEqual(mock_get.call_count, 5)
        self.assertEqual(mock_sleep.call_count, 5)

    def test_enqueue_dhd_posts_stops_when_first_page_has_no_new_items(self):
        """第一页全部是旧项目时，应停止抓取并保留原 newest_id。"""
        with patch.object(
            self.module, "fetch_dhd_batch", return_value=[{"id": "100", "name": "Old", "url": "u"}]
        ) as mock_fetch, patch.object(
            self.module, "update_json_config"
        ) as mock_update:
            self.module.enqueue_dhd_posts(start_page=1, redis_client=self.redis_client)

        mock_fetch.assert_called_once_with(1, 1)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 0)
        mock_update.assert_called_once_with("config/scrapy_dhd.json", "newest_id", 100)

    def test_enqueue_dhd_posts_processes_multiple_pages_and_updates_max_id(self):
        """多页翻页时应把新帖子写入 Redis 队列，并回写本轮最大 ID。"""
        parsed_pages = [
            [
                {"id": "101", "name": "A", "url": "https://example.com/topic/101"},
                {"id": "105", "name": "B", "url": "https://example.com/topic/105"},
            ],
            [
                {"id": "103", "name": "C", "url": "https://example.com/topic/103"},
            ],
            [
                {"id": "100", "name": "Old", "url": "https://example.com/topic/100"},
            ],
        ]

        with patch.object(
            self.module, "fetch_dhd_batch", side_effect=parsed_pages
        ) as mock_fetch, patch.object(
            self.module, "update_json_config"
        ) as mock_update:
            self.module.enqueue_dhd_posts(start_page=1, redis_client=self.redis_client)

        self.assertEqual(
            mock_fetch.call_args_list,
            [
                call(1, 1),
                call(2, 1),
                call(3, 1),
            ],
        )
        pending_payloads = self.redis_client.lrange(self.module.REDIS_PENDING_KEY, 0, -1)
        pending_ids = [self.module.deserialize_dhd_info(payload)["id"] for payload in pending_payloads]
        self.assertEqual(pending_ids, ["101", "105", "103"])
        mock_update.assert_called_once_with("config/scrapy_dhd.json", "newest_id", 105)

    def test_recover_dhd_processing_queue_moves_items_back_to_pending(self):
        """中断残留在 processing 的任务应恢复回 pending。"""
        payload_a = self.module.serialize_dhd_info({"id": "101", "name": "A", "url": "u1"})
        payload_b = self.module.serialize_dhd_info({"id": "102", "name": "B", "url": "u2"})
        payload_c = self.module.serialize_dhd_info({"id": "103", "name": "C", "url": "u3"})
        self.redis_client.rpush(self.module.REDIS_PENDING_KEY, payload_a)
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, payload_b)
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, payload_c)

        recovered_count = self.module.recover_dhd_processing_queue(self.redis_client)

        self.assertEqual(recovered_count, 2)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PROCESSING_KEY), 0)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 3)

    def test_drain_dhd_queue_processes_pending_items_and_records_failures(self):
        """消费队列时，成功任务应清理 processing，失败任务应进入 failed。"""
        payload_success = self.module.serialize_dhd_info(
            {"id": "101", "name": "A", "url": "https://example.com/topic/101"}
        )
        payload_fail = self.module.serialize_dhd_info(
            {"id": "102", "name": "B", "url": "https://example.com/topic/102"}
        )
        self.redis_client.rpush(self.module.REDIS_PENDING_KEY, payload_success)
        self.redis_client.rpush(self.module.REDIS_PENDING_KEY, payload_fail)

        def fake_working_dhd(info: dict) -> None:
            if info["id"] == "102":
                raise RuntimeError("boom")

        with patch.object(self.module, "working_dhd", side_effect=fake_working_dhd) as mock_working, patch.object(
            self.module.logger, "error"
        ) as mock_error:
            self.module.drain_dhd_queue(redis_client=self.redis_client)

        self.assertEqual(mock_working.call_count, 2)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 0)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PROCESSING_KEY), 0)
        failed_payloads = self.redis_client.lrange(self.module.REDIS_FAILED_KEY, 0, -1)
        self.assertEqual(len(failed_payloads), 1)
        self.assertEqual(self.module.deserialize_dhd_info(failed_payloads[0])["id"], "102")
        self.assertIn("https://example.com/topic/102", mock_error.call_args[0][0])

    def test_drain_dhd_queue_logs_when_queue_is_empty(self):
        """没有待处理任务时，应输出空队列提示并直接返回。"""
        with patch.object(self.module.logger, "info") as mock_info:
            self.module.drain_dhd_queue(redis_client=self.redis_client)

        self.assertIn("DHD 队列为空，没有待处理任务", [call.args[0] for call in mock_info.call_args_list])


class TestDhdToLog(unittest.TestCase):
    """验证 ``.dhd`` 转 ``.log`` 的目录处理逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_dhd()
        self.base_dir = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_dhd_to_log_renames_file_and_writes_magnet_on_success(self):
        """成功转换时应生成 ``.log``，删除原 ``.dhd`` 和临时 ``.torrent``。"""
        source_file = self.base_dir / "movie.dhd"
        source_file.write_text("https://example.com/topic/123\n", encoding="utf-8")

        def fake_get_dhd_torrent(_session, _url: str, torrent_path: str) -> None:
            Path(torrent_path).write_bytes(b"fake torrent")

        with patch.object(
            self.module,
            "get_dhd",
            return_value=Mock(text='<p class="attnm"><a href="download.php?id=123">torrent</a></p>'),
        ), patch.object(
            self.module, "get_dhd_torrent", side_effect=fake_get_dhd_torrent
        ), patch.object(
            self.module, "torrent_to_magnet", return_value="magnet:?xt=urn:btih:test"
        ):
            self.module.dhd_to_log(str(self.base_dir))

        log_path = self.base_dir / "movie.log"
        torrent_path = self.base_dir / "movie.torrent"
        self.assertFalse(source_file.exists())
        self.assertTrue(log_path.exists())
        self.assertEqual(log_path.read_text(encoding="utf-8"), "magnet:?xt=urn:btih:test")
        self.assertFalse(torrent_path.exists())

    def test_dhd_to_log_keeps_source_file_when_download_link_is_missing(self):
        """抓不到下载地址时，应保留原 ``.dhd``，也不应生成 ``.log``。"""
        source_file = self.base_dir / "movie.dhd"
        source_file.write_text("https://example.com/topic/123\n", encoding="utf-8")

        with patch.object(self.module, "get_dhd", return_value=Mock(text="<html></html>")), patch.object(
            self.module, "get_dhd_torrent"
        ) as mock_download, patch.object(self.module, "torrent_to_magnet") as mock_magnet:
            self.module.dhd_to_log(str(self.base_dir))

        self.assertTrue(source_file.exists())
        self.assertFalse((self.base_dir / "movie.log").exists())
        mock_download.assert_not_called()
        mock_magnet.assert_not_called()

    def test_dhd_to_log_stops_when_write_log_fails(self):
        """写日志失败时，不应继续记录转换完成。"""
        source_file = self.base_dir / "movie.dhd"
        source_file.write_text("https://example.com/topic/123\n", encoding="utf-8")
        torrent_path = self.base_dir / "movie.torrent"
        log_path = self.base_dir / "movie.log"

        def fake_get_dhd_torrent(_session, _url: str, torrent_path: str) -> None:
            Path(torrent_path).write_bytes(b"fake torrent")

        with patch.object(
            self.module,
            "get_dhd",
            return_value=Mock(text='<p class="attnm"><a href="download.php?id=123">torrent</a></p>'),
        ), patch.object(
            self.module, "get_dhd_torrent", side_effect=fake_get_dhd_torrent
        ), patch.object(
            self.module, "torrent_to_magnet", return_value="magnet:?xt=urn:btih:test"
        ), patch.object(
            self.module, "write_list_to_file", return_value=False
        ), patch.object(self.module.logger, "info") as mock_info:
            self.module.dhd_to_log(str(self.base_dir))

        self.assertTrue(source_file.exists())
        self.assertTrue(torrent_path.exists())
        self.assertFalse(log_path.exists())
        self.assertNotIn(f"文件 {source_file}: 转换完成", [call.args[0] for call in mock_info.call_args_list])

    def test_dhd_to_log_deletes_temp_torrent_when_magnet_conversion_raises(self):
        """连续三次坏种子异常时，应保留 ``.dhd``，且删除临时 ``.torrent``。"""
        source_file = self.base_dir / "movie.dhd"
        source_file.write_text("https://example.com/topic/123\n", encoding="utf-8")
        torrent_path = self.base_dir / "movie.torrent"
        log_path = self.base_dir / "movie.log"

        def fake_get_dhd_torrent(_session, _url: str, torrent_path: str) -> None:
            Path(torrent_path).write_bytes(b"fake torrent")

        with patch.object(
            self.module,
            "get_dhd",
            return_value=Mock(text='<p class="attnm"><a href="download.php?id=123">torrent</a></p>'),
        ), patch.object(
            self.module, "get_dhd_torrent", side_effect=fake_get_dhd_torrent
        ), patch.object(
            self.module, "torrent_to_magnet", side_effect=ValueError("bad torrent")
        ) as mock_magnet, patch.object(self.module.logger, "error") as mock_error:
            self.module.dhd_to_log(str(self.base_dir))

        self.assertTrue(source_file.exists())
        self.assertFalse(torrent_path.exists())
        self.assertFalse(log_path.exists())
        self.assertEqual(mock_magnet.call_count, 3)
        self.assertIn("连续 3 次下载到无效种子", mock_error.call_args[0][0])

    def test_dhd_to_log_deletes_temp_torrent_when_magnet_conversion_returns_empty(self):
        """连续三次空磁链结果时，应保留 ``.dhd``，且删除临时 ``.torrent``。"""
        source_file = self.base_dir / "movie.dhd"
        source_file.write_text("https://example.com/topic/123\n", encoding="utf-8")
        torrent_path = self.base_dir / "movie.torrent"
        log_path = self.base_dir / "movie.log"

        def fake_get_dhd_torrent(_session, _url: str, torrent_path: str) -> None:
            Path(torrent_path).write_bytes(b"fake torrent")

        with patch.object(
            self.module,
            "get_dhd",
            return_value=Mock(text='<p class="attnm"><a href="download.php?id=123">torrent</a></p>'),
        ), patch.object(
            self.module, "get_dhd_torrent", side_effect=fake_get_dhd_torrent
        ), patch.object(
            self.module, "torrent_to_magnet", return_value=""
        ) as mock_magnet, patch.object(self.module.logger, "error") as mock_error:
            self.module.dhd_to_log(str(self.base_dir))

        self.assertTrue(source_file.exists())
        self.assertFalse(torrent_path.exists())
        self.assertFalse(log_path.exists())
        self.assertEqual(mock_magnet.call_count, 3)
        self.assertIn("连续 3 次下载到无效种子", mock_error.call_args[0][0])

    def test_dhd_to_log_retries_bad_torrent_and_succeeds(self):
        """坏种子前两次失败、第三次成功时，应最终写出 ``.log``。"""
        source_file = self.base_dir / "movie.dhd"
        source_file.write_text("https://example.com/topic/123\n", encoding="utf-8")
        torrent_path = self.base_dir / "movie.torrent"
        log_path = self.base_dir / "movie.log"

        def fake_get_dhd_torrent(_session, _url: str, torrent_path: str) -> None:
            Path(torrent_path).write_bytes(b"fake torrent")

        magnet_results = [ValueError("bad torrent"), ValueError("bad torrent"), "magnet:?xt=urn:btih:ok"]

        def fake_torrent_to_magnet(_torrent_path: str) -> str:
            result = magnet_results.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        with patch.object(
            self.module,
            "get_dhd",
            return_value=Mock(text='<p class="attnm"><a href="download.php?id=123">torrent</a></p>'),
        ), patch.object(
            self.module, "get_dhd_torrent", side_effect=fake_get_dhd_torrent
        ) as mock_download, patch.object(
            self.module, "torrent_to_magnet", side_effect=fake_torrent_to_magnet
        ) as mock_magnet:
            self.module.dhd_to_log(str(self.base_dir))

        self.assertFalse(source_file.exists())
        self.assertFalse(torrent_path.exists())
        self.assertTrue(log_path.exists())
        self.assertEqual(log_path.read_text(encoding="utf-8"), "magnet:?xt=urn:btih:ok")
        self.assertEqual(mock_download.call_count, 3)
        self.assertEqual(mock_magnet.call_count, 3)

    def test_dhd_to_log_logs_file_path_when_worker_raises(self):
        """线程任务抛异常时，汇总日志应带上对应文件路径。"""
        source_file = self.base_dir / "movie.dhd"
        source_file.write_text("https://example.com/topic/123\n", encoding="utf-8")

        with patch.object(self.module, "process_dhd_file", side_effect=RuntimeError("boom")), patch.object(
            self.module.logger, "error"
        ) as mock_error:
            self.module.dhd_to_log(str(self.base_dir))

        self.assertIn(str(source_file), mock_error.call_args[0][0])
        self.assertIn("boom", mock_error.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
