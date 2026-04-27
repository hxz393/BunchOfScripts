"""
针对 ``my_scripts.scrapy_hde`` 的单元测试。

这里在隔离环境中加载模块，不依赖真实配置、Redis 或网络请求。
主要验证请求、列表页解析、内容保护解锁、详情页落盘，以及 Redis 两段式调度与 ``end_titles`` 回写。
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

import requests

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_hde.py"


class FakeRedis:
    """最小 Redis 替身，覆盖当前测试需要的键、列表和集合操作。"""

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
            self.sets.pop(key, None)

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


def fake_serialize_payload(payload: dict) -> str:
    """与真实 helper 保持一致的 JSON 序列化。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def fake_deserialize_payload(payload: str) -> dict:
    """与真实 helper 保持一致的 JSON 反序列化。"""
    return json.loads(payload)


def fake_push_items_to_queue(
        redis_client: FakeRedis,
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


def load_scrapy_hde(config: dict | None = None):
    """在最小依赖环境中加载 ``scrapy_hde`` 模块。"""
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "hde_url": "https://example.com/",
        "output_dir": str(Path(temp_dir.name) / "downloads"),
        "end_titles": ["Old Movie – 1.0 GB", "Older Movie – 0.9 GB"],
        "max_workers": 30,
        "default_release_size": "100.0 GB",
        "request_timeout_seconds": 30,
        "retry_max_attempts": 150,
        "retry_wait_min_ms": 1000,
        "retry_wait_max_ms": 10000,
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
        f"scrapy_hde_test_{uuid.uuid4().hex}",
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


def build_fit_item(title: str, href: str) -> str:
    """构造一个最小可用的列表页条目。"""
    return f"""
    <div class="fit item">
      <div class="data">
        <h5><a href="{href}">{title}</a></h5>
      </div>
    </div>
    """


def build_list_page(*items: str) -> str:
    """构造一个最小可用的列表页 HTML。"""
    return f"<html><body>{''.join(items)}</body></html>"


def build_detail_page(*hrefs: str) -> str:
    """构造一个最小可用的详情页 HTML。"""
    return "<html><body>" + "".join(f'<a href="{href}">id</a>' for href in hrefs) + "</body></html>"


def build_protected_detail_page(
    imdb_href: str = "https://www.imdb.com/title/tt1234567/",
    token: str = "token-value",
    ident: str = "ident-value",
    chax_response: str = "chax-value",
) -> str:
    """构造带内容保护表单的详情页 HTML。"""
    return f"""
    <html>
      <body>
        <div class="entry-content">
          <a href="{imdb_href}">IMDb</a>
          <form method="post" action="/sample/#unlocked">
            <input type="hidden" name="content-protector-captcha" value="1" />
            <input type="hidden" name="content-protector-token" value="{token}" />
            <input type="hidden" name="content-protector-ident" value="{ident}" />
            <input type="hidden" name="chax-response" value="{chax_response}" />
            <input type="submit" name="content-protector-submit" value="Access the links" />
          </form>
        </div>
      </body>
    </html>
    """


def build_unlocked_detail_page(*hrefs: str) -> str:
    """构造已解锁的详情页正文 HTML。"""
    return '<div class="entry-content">' + "".join(f'<a href="{href}">{href}</a>' for href in hrefs) + '</div>'


class TestModuleLoad(unittest.TestCase):
    """验证模块导入时的配置注入。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_scrapy_hde_uses_injected_config(self):
        """模块加载后应暴露注入的站点地址、输出目录和配置化默认值。"""
        self.assertEqual(self.module.HDE_URL, "https://example.com/")
        self.assertTrue(str(self.module.OUTPUT_DIR).endswith("downloads"))
        self.assertEqual(self.module.END_TITLES, ["Old Movie – 1.0 GB", "Older Movie – 0.9 GB"])
        self.assertEqual(self.module.DEFAULT_MAX_WORKERS, 30)


class TestHdeHelpers(unittest.TestCase):
    """验证主流程辅助函数。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_build_hde_page_url_formats_archive_page_address(self):
        """分页 URL 应基于配置的站点前缀拼接。"""
        self.assertEqual(
            self.module.build_hde_page_url(3),
            "https://example.com/tag/movies/page/3/",
        )

    def test_should_stop_scrapy_returns_true_only_when_end_title_is_present(self):
        """任一截止标题存在时应停止翻页，否则继续。"""
        result_list = [
            {"title": "New Movie – 2.0 GB", "url": "https://example.com/new", "size": "2.0GB"},
            {"title": "Old Movie – 1.0 GB", "url": "https://example.com/old", "size": "1.0GB"},
        ]

        self.assertTrue(self.module.should_stop_scrapy(result_list, ["Old Movie – 1.0 GB", "Older Movie – 0.9 GB"]))
        self.assertFalse(self.module.should_stop_scrapy(result_list, ["Missing Movie", "Another Missing Movie"]))

    def test_select_next_end_titles_returns_first_two_titles_from_first_page(self):
        """下一轮截止标题应取首次访问页最前面的两个标题。"""
        result_list = [
            {"title": "Newest Movie – 2.0 GB", "url": "https://example.com/newest", "size": "2.0GB"},
            {"title": "Second Movie – 1.8 GB", "url": "https://example.com/second", "size": "1.8GB"},
            {"title": "Third Movie – 1.0 GB", "url": "https://example.com/third", "size": "1.0GB"},
        ]

        self.assertEqual(
            self.module.select_next_end_titles(result_list),
            ["Newest Movie – 2.0 GB", "Second Movie – 1.8 GB"],
        )

    def test_get_current_end_titles_returns_configured_titles(self):
        """当前截止标题列表应来自配置。"""
        self.assertEqual(
            self.module.get_current_end_titles(),
            ["Old Movie – 1.0 GB", "Older Movie – 0.9 GB"],
        )

    def test_find_hde_protected_form_returns_matching_form(self):
        """带保护字段的表单应被正确定位。"""
        soup = self.module.BeautifulSoup(build_protected_detail_page(), "html.parser")

        form = self.module.find_hde_protected_form(soup)

        self.assertIsNotNone(form)
        self.assertEqual(form.get("method"), "post")

    def test_build_hde_protected_form_payload_collects_hidden_and_submit_inputs(self):
        """回发表单时应只保留隐藏字段和提交按钮。"""
        soup = self.module.BeautifulSoup(build_protected_detail_page(), "html.parser")
        form = self.module.find_hde_protected_form(soup)

        payload = self.module.build_hde_protected_form_payload(form)

        self.assertEqual(
            payload,
            [
                ("content-protector-captcha", "1"),
                ("content-protector-token", "token-value"),
                ("content-protector-ident", "ident-value"),
                ("chax-response", "chax-value"),
                ("content-protector-submit", "Access the links"),
            ],
        )

    def test_unlock_hde_protected_soup_posts_form_and_returns_unlocked_page(self):
        """存在保护表单时应自动 POST 解锁，并返回解锁后的 soup。"""
        session = self.module.requests.Session()
        protected_soup = self.module.BeautifulSoup(build_protected_detail_page(), "html.parser")
        unlocked_response = Mock(
            text=build_unlocked_detail_page(
                "https://www.imdb.com/title/tt1234567/",
                "https://rapidgator.net/file/sample.rar.html",
            )
        )

        with patch.object(self.module, "post_hde_response", return_value=unlocked_response) as mock_post:
            unlocked_soup = self.module.unlock_hde_protected_soup(
                "https://example.com/post",
                protected_soup,
                session=session,
            )

        self.assertIsNotNone(unlocked_soup.select_one("div.entry-content"))
        mock_post.assert_called_once_with(
            "https://example.com/post",
            [
                ("content-protector-captcha", "1"),
                ("content-protector-token", "token-value"),
                ("content-protector-ident", "ident-value"),
                ("chax-response", "chax-value"),
                ("content-protector-submit", "Access the links"),
            ],
            session,
        )

    def test_extract_hde_useful_links_keeps_imdb_and_downloads_but_skips_images(self):
        """应保留 IMDb 与下载链接，过滤截图和邮箱保护链接。"""
        soup = self.module.BeautifulSoup(
            """
            <div class="entry-content">
              <a href="https://www.imdb.com/title/tt1234567/">IMDb</a>
              <a href="/cdn-cgi/l/email-protection">mail</a>
              <a href="https://img2.pixhost.to/images/1/sample.png">image</a>
              <a href="https://rapidgator.net/file/sample.rar.html">RG</a>
              <a href="https://nitroflare.com/view/sample">NF</a>
            </div>
            """,
            "html.parser",
        )

        result = self.module.extract_hde_useful_links(soup)

        self.assertEqual(
            result,
            [
                "https://www.imdb.com/title/tt1234567/",
                "https://rapidgator.net/file/sample.rar.html",
                "https://nitroflare.com/view/sample",
            ],
        )

    def test_build_hde_output_content_prepends_detail_url_and_deduped_useful_links(self):
        """写盘内容应以详情页 URL 开头，其后跟 IMDb 与解锁后的有效链接。"""
        soup = self.module.BeautifulSoup(
            build_unlocked_detail_page(
                "https://www.imdb.com/title/tt1234567/",
                "https://rapidgator.net/file/sample.rar.html",
            ),
            "html.parser",
        )

        result = self.module.build_hde_output_content("https://example.com/post", "tt1234567", soup)

        self.assertEqual(
            result,
            [
                "https://example.com/post",
                "https://www.imdb.com/title/tt1234567/",
                "https://rapidgator.net/file/sample.rar.html",
            ],
        )


class TestGetHdeResponse(unittest.TestCase):
    """验证单次请求逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_hde_response_returns_response_and_sets_utf8_encoding(self):
        """请求成功时应返回响应对象，并统一设置 UTF-8 编码。"""
        response = Mock(status_code=200)

        with patch.object(self.module.session, "get", return_value=response) as mock_get:
            result = self.module.get_hde_response("https://example.com/post")

        self.assertIs(result, response)
        self.assertEqual(response.encoding, "utf-8")
        mock_get.assert_called_once_with("https://example.com/post", timeout=30, verify=False)

    def test_get_hde_response_raises_when_status_code_is_not_200(self):
        """请求返回非 200 状态码时应抛出异常。"""
        response = Mock(status_code=503)

        with patch.object(self.module.session, "get", return_value=response):
            with self.assertRaisesRegex(Exception, "503"):
                self.module.get_hde_response("https://example.com/post")

    def test_get_hde_response_propagates_request_exception(self):
        """底层请求异常时应直接抛出，交给重试装饰器处理。"""
        with patch.object(self.module.session, "get", side_effect=requests.Timeout("timed out")):
            with self.assertRaisesRegex(requests.Timeout, "timed out"):
                self.module.get_hde_response("https://example.com/post")


class TestParseHdeResponse(unittest.TestCase):
    """验证列表页解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_hde_response_extracts_title_and_url(self):
        """应从列表页提取标题、链接和体积。"""
        response = Mock(
            text=build_list_page(
                build_fit_item(
                    title="Movie Title – 1.2 GB",
                    href="https://example.com/post-1",
                )
            )
        )

        result = self.module.parse_hde_response(response)

        self.assertEqual(
            result,
            [
                {
                    "title": "Movie Title – 1.2 GB",
                    "url": "https://example.com/post-1",
                    "size": "1.2GB",
                }
            ],
        )

    def test_parse_hde_response_skips_entries_without_data_or_anchor(self):
        """条目缺少 ``div.data`` 或标题链接时应跳过。"""
        response = Mock(
            text=build_list_page(
                '<div class="fit item"><div class="other"></div></div>',
                '<div class="fit item"><div class="data"><h5>No link</h5></div></div>',
                build_fit_item(
                    title="Valid Movie",
                    href="https://example.com/post-2",
                ),
            )
        )

        result = self.module.parse_hde_response(response)

        self.assertEqual(
            result,
            [
                {
                    "title": "Valid Movie",
                    "url": "https://example.com/post-2",
                    "size": "100.0GB",
                }
            ],
        )

    def test_parse_hde_item_returns_none_when_required_nodes_are_missing(self):
        """单条条目缺少 ``div.data`` 或链接时应返回 ``None``。"""
        fit_without_data = self.module.BeautifulSoup(
            '<div class="fit item"><div class="other"></div></div>',
            "html.parser",
        ).select_one("div.fit.item")
        fit_without_link = self.module.BeautifulSoup(
            '<div class="fit item"><div class="data"><h5>No link</h5></div></div>',
            "html.parser",
        ).select_one("div.fit.item")

        self.assertIsNone(self.module.parse_hde_item(fit_without_data))
        self.assertIsNone(self.module.parse_hde_item(fit_without_link))


class TestReleaseSize(unittest.TestCase):
    """验证大小提取逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_extract_release_size_extracts_size_from_dash_suffix_or_trailing_size(self):
        """应优先提取破折号后的大小，其次回退到末尾大小。"""
        self.assertEqual(self.module.extract_release_size("Movie One – 22.4 GB"), "22.4GB")
        self.assertEqual(self.module.extract_release_size("Movie Two 700 MB"), "700MB")

    def test_extract_release_size_returns_normalized_default_when_size_is_missing(self):
        """没有体积信息时应返回去空格后的默认值。"""
        self.assertEqual(self.module.extract_release_size("Movie Three"), "100.0GB")
        self.assertEqual(self.module.extract_release_size("Movie Three", default_size="1.5 TB"), "1.5TB")


class TestVisitHdeUrl(unittest.TestCase):
    """验证详情页访问和写盘逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_visit_hde_url_extracts_imdb_and_writes_release_file(self):
        """详情页包含 IMDb 链接时，应提取 ID、解锁链接并按规则落盘。"""
        result_item = {
            "title": "Movie / Title: 2026",
            "url": "https://example.com/post",
            "size": "22.4GB",
        }
        detail_session = Mock()
        response = Mock(text=build_detail_page("https://www.imdb.com/title/tt1234567/"))
        unlocked_soup = self.module.BeautifulSoup(
            build_unlocked_detail_page(
                "https://www.imdb.com/title/tt1234567/",
                "https://rapidgator.net/file/sample.rar.html",
            ),
            "html.parser",
        )

        with patch.object(self.module, "build_hde_session", return_value=detail_session) as mock_session, patch.object(
            self.module,
            "get_hde_response",
            return_value=response,
        ) as mock_get, patch.object(
            self.module,
            "unlock_hde_protected_soup",
            return_value=unlocked_soup,
        ) as mock_unlock, patch.object(
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
            self.module.visit_hde_url(result_item)

        self.assertEqual(result_item["imdb"], "tt1234567")
        mock_session.assert_called_once_with()
        mock_get.assert_called_once_with("https://example.com/post", session=detail_session)
        mock_unlock.assert_called_once_with("https://example.com/post", ANY, detail_session)
        mock_normalize.assert_called_once_with("Movie / Title: 2026")
        mock_sanitize.assert_called_once_with("Normalized / Title: 2026")
        mock_write.assert_called_once_with(
            str(Path(self.module.OUTPUT_DIR) / "Sanitized Title 2026 - hde (22.4GB)[tt1234567].rls"),
            [
                "https://example.com/post",
                "https://www.imdb.com/title/tt1234567/",
                "https://rapidgator.net/file/sample.rar.html",
            ],
        )

    def test_visit_hde_url_falls_back_to_loose_tt_match_when_imdb_link_is_noncanonical(self):
        """详情页没有标准 IMDb URL 时，仍应从其它链接中回退提取 ``tt`` 编号。"""
        result_item = {
            "title": "Fallback Title",
            "url": "https://example.com/post",
            "size": "1.0GB",
        }
        detail_session = Mock()
        response = Mock(text=build_detail_page("https://example.com/redirect?target=tt7654321"))

        with patch.object(self.module, "build_hde_session", return_value=detail_session), patch.object(
            self.module,
            "get_hde_response",
            return_value=response,
        ), patch.object(
            self.module,
            "unlock_hde_protected_soup",
            return_value=self.module.BeautifulSoup("<div></div>", "html.parser"),
        ), patch.object(
            self.module,
            "write_list_to_file",
            return_value=True,
        ):
            self.module.visit_hde_url(result_item)

        self.assertEqual(result_item["imdb"], "tt7654321")

    def test_extract_imdb_id_from_links_prefers_canonical_imdb_url(self):
        """同时存在多种链接时，应优先取标准 IMDb 标题页。"""
        imdb_id = self.module.extract_imdb_id_from_links(
            [
                "https://example.com/redirect?target=tt7654321",
                "https://www.imdb.com/title/tt1234567/",
            ]
        )

        self.assertEqual(imdb_id, "tt1234567")

    def test_build_hde_output_filename_uses_sanitized_title_size_and_imdb(self):
        """输出文件名应基于标题、体积和 IMDb 编号拼接。"""
        result_item = {
            "title": "Movie / Title: 2026",
            "size": "22.4GB",
            "imdb": "tt1234567",
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
            file_name = self.module.build_hde_output_filename(result_item)

        self.assertEqual(file_name, "Sanitized Title 2026 - hde (22.4GB)[tt1234567].rls")
        mock_normalize.assert_called_once_with("Movie / Title: 2026")
        mock_sanitize.assert_called_once_with("Normalized / Title: 2026")


class TestEnqueueHdePosts(unittest.TestCase):
    """验证列表扫描入队和断点逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_enqueue_hde_posts_stops_when_end_title_is_found_on_first_page(self):
        """第一页命中截止标题时应完成扫描、写入下一轮标题并把条目入队。"""
        response = Mock()
        result_list = [
            {"title": "Old Movie – 1.0 GB", "url": "https://example.com/post-1", "size": "1.0GB"},
            {"title": "Older Movie – 0.9 GB", "url": "https://example.com/post-2", "size": "0.9GB"},
        ]

        with patch.object(self.module, "get_hde_response", return_value=response) as mock_get, patch.object(
            self.module,
            "parse_hde_response",
            return_value=result_list,
        ) as mock_parse:
            self.module.enqueue_hde_posts(start_page=3, redis_client=self.redis_client)

        mock_get.assert_called_once_with("https://example.com/tag/movies/page/3/")
        mock_parse.assert_called_once_with(response)
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY), "1")
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_PAGE_KEY), "4")
        self.assertEqual(
            self.module.deserialize_payload(self.redis_client.get(self.module.REDIS_NEXT_END_TITLES_KEY))["titles"],
            ["Old Movie – 1.0 GB", "Older Movie – 0.9 GB"],
        )
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 2)
        self.assertEqual(
            self.module.deserialize_payload(self.redis_client.lists[self.module.REDIS_PENDING_KEY][0]),
            {"size": "1.0GB", "title": "Old Movie – 1.0 GB", "url": "https://example.com/post-1"},
        )

    def test_enqueue_hde_posts_moves_to_next_page_until_end_title_is_found(self):
        """未命中截止标题时应继续抓取下一页，且下一轮标题只记录第一页。"""
        first_response = Mock()
        second_response = Mock()
        first_result_list = [
            {"title": "New Movie – 2.0 GB", "url": "https://example.com/new", "size": "2.0GB"},
            {"title": "Second Movie – 1.8 GB", "url": "https://example.com/second", "size": "1.8GB"},
        ]
        second_result_list = [{"title": "Old Movie – 1.0 GB", "url": "https://example.com/old", "size": "1.0GB"}]

        with patch.object(
            self.module,
            "get_hde_response",
            side_effect=[first_response, second_response],
        ) as mock_get, patch.object(
            self.module,
            "parse_hde_response",
            side_effect=[first_result_list, second_result_list],
        ) as mock_parse:
            self.module.enqueue_hde_posts(start_page=3, redis_client=self.redis_client)

        self.assertEqual(
            mock_get.call_args_list,
            [
                call("https://example.com/tag/movies/page/3/"),
                call("https://example.com/tag/movies/page/4/"),
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


class TestRecoverAndDrainHdeQueue(unittest.TestCase):
    """验证 Redis 队列恢复与消费编排。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_recover_hde_processing_when_pending_is_empty_moves_payloads_back(self):
        """待处理为空且处理中有残留时，应回退到待处理队列。"""
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, "task-1", "task-2")

        recovered = self.module.recover_hde_processing_when_pending_is_empty(self.redis_client)

        self.assertEqual(recovered, 2)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PROCESSING_KEY), 0)
        self.assertEqual(self.redis_client.lists[self.module.REDIS_PENDING_KEY], ["task-1", "task-2"])

    def test_drain_hde_queue_delegates_to_shared_drain_queue_helper(self):
        """消费阶段应把队列参数和 worker 函数交给共享 helper。"""
        with patch.object(self.module, "drain_queue") as mock_drain:
            self.module.drain_hde_queue(redis_client=self.redis_client)

        mock_drain.assert_called_once_with(
            self.redis_client,
            pending_key=self.module.REDIS_PENDING_KEY,
            processing_key=self.module.REDIS_PROCESSING_KEY,
            max_workers=self.module.DEFAULT_MAX_WORKERS,
            worker=self.module.visit_hde_url,
            deserialize=self.module.deserialize_payload,
            logger=self.module.logger,
            queue_label="HDE",
            identify_item=ANY,
            recover_processing_on_start=False,
            keep_failed_in_processing=True,
        )


class TestFinalizeHdeRun(unittest.TestCase):
    """验证回写截止标题和清理状态的逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_finalize_hde_run_updates_end_titles_after_scan_and_queue_finish(self):
        """列表扫描完成且队列清空后，应回写新的截止标题并清理运行状态。"""
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")
        self.redis_client.set(
            self.module.REDIS_NEXT_END_TITLES_KEY,
            self.module.serialize_payload({"titles": ["Newest Movie", "Second Movie"]}),
        )
        self.redis_client.set(self.module.REDIS_SCAN_PAGE_KEY, "8")

        with patch.object(self.module, "update_json_config") as mock_update:
            self.module.finalize_hde_run(redis_client=self.redis_client)

        mock_update.assert_called_once_with(
            self.module.CONFIG_PATH,
            "end_titles",
            ["Newest Movie", "Second Movie"],
        )
        self.assertIsNone(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY))
        self.assertIsNone(self.redis_client.get(self.module.REDIS_SCAN_PAGE_KEY))
        self.assertIsNone(self.redis_client.get(self.module.REDIS_NEXT_END_TITLES_KEY))

    def test_finalize_hde_run_skips_update_when_processing_queue_is_not_empty(self):
        """处理中仍有残留时，不应回写截止标题。"""
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")
        self.redis_client.set(
            self.module.REDIS_NEXT_END_TITLES_KEY,
            self.module.serialize_payload({"titles": ["Newest Movie", "Second Movie"]}),
        )
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, "task-1")

        with patch.object(self.module, "update_json_config") as mock_update:
            self.module.finalize_hde_run(redis_client=self.redis_client)

        mock_update.assert_not_called()
        self.assertIsNotNone(self.redis_client.get(self.module.REDIS_NEXT_END_TITLES_KEY))


class TestScrapyHdeMain(unittest.TestCase):
    """验证主入口调度逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_hde()
        self.redis_client = FakeRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_hde_runs_recover_enqueue_drain_and_finalize_in_order(self):
        """主入口应先恢复，再入队、消费，并在 finally 中执行收尾。"""
        with patch.object(self.module, "get_redis_client", return_value=self.redis_client) as mock_get_redis, patch.object(
            self.module,
            "recover_hde_processing_when_pending_is_empty",
        ) as mock_recover, patch.object(
            self.module,
            "enqueue_hde_posts",
        ) as mock_enqueue, patch.object(
            self.module,
            "drain_hde_queue",
        ) as mock_drain, patch.object(
            self.module,
            "finalize_hde_run",
        ) as mock_finalize:
            self.module.scrapy_hde(start_page=5)

        mock_get_redis.assert_called_once_with()
        mock_recover.assert_called_once_with(self.redis_client)
        mock_enqueue.assert_called_once_with(start_page=5, redis_client=self.redis_client)
        mock_drain.assert_called_once_with(redis_client=self.redis_client)
        mock_finalize.assert_called_once_with(redis_client=self.redis_client)

    def test_scrapy_hde_still_finalizes_when_drain_queue_raises(self):
        """消费阶段异常时，收尾逻辑仍应执行。"""
        with patch.object(self.module, "get_redis_client", return_value=self.redis_client), patch.object(
            self.module,
            "recover_hde_processing_when_pending_is_empty",
        ), patch.object(
            self.module,
            "enqueue_hde_posts",
        ), patch.object(
            self.module,
            "drain_hde_queue",
            side_effect=RuntimeError("boom"),
        ), patch.object(
            self.module,
            "finalize_hde_run",
        ) as mock_finalize:
            with self.assertRaisesRegex(RuntimeError, "boom"):
                self.module.scrapy_hde(start_page=5)

        mock_finalize.assert_called_once_with(redis_client=self.redis_client)


if __name__ == "__main__":
    unittest.main()
