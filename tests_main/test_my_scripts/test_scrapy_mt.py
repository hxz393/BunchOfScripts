"""
针对 ``my_scripts.scrapy_mt`` 的单元测试。

这里在隔离环境中加载模块，不依赖真实配置，也不会发出真实网络请求。
主要验证：
1. 模块导入时的配置注入。
2. 自动时间窗口与重试策略辅助函数。
3. ``post_mt_response`` 的请求参数、编码设置和异常分支。
4. ``parse_mt_response`` 的文件名拼装与落盘行为。
5. ``scrapy_mt`` 主流程的分页控制、成功回写和失败保护。
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

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_mt.py"


class DummyRetry:
    """避免测试依赖 urllib3 具体版本。"""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class DummyHTTPAdapter:
    """避免测试依赖 requests/urllib3 适配器内部实现。"""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


def load_scrapy_mt(config: dict | None = None):
    """
    在隔离环境中加载 ``scrapy_mt`` 模块。

    被测模块在 import 时就会读取配置、构造 ``Retry`` / ``HTTPAdapter``，
    所以这里先注入假的依赖，避免测试依赖真实配置和第三方库版本细节。
    """
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "mt_api_url": "https://example.com/api/torrent/search",
        "mt_auth": "Bearer test-token",
        "mt_sign": "unit-sign",
        "mt_time": "1700000000",
        "request_head": {"User-Agent": "unit-test"},
        "query_time": "2026-03-25",
        "output_dir": temp_dir.name,
    }
    if config:
        module_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: copy.deepcopy(module_config)
    fake_my_module.format_size = lambda value: f"{value} B"
    fake_my_module.normalize_release_title_for_filename = lambda title, **_kwargs: title.replace("/", "｜")
    fake_my_module.sanitize_filename = lambda name: name.replace(":", "_")

    def fake_write_dict_to_json(path: str, data: dict) -> None:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    fake_my_module.write_dict_to_json = fake_write_dict_to_json
    fake_my_module.update_json_config = lambda _path, key, value: module_config.__setitem__(key, value)

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    spec = importlib.util.spec_from_file_location(
        f"scrapy_mt_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "retrying": fake_retrying,
        },
    ), patch("requests.adapters.HTTPAdapter", DummyHTTPAdapter), patch(
        "urllib3.util.retry.Retry", DummyRetry
    ):
        spec.loader.exec_module(module)

    return module, temp_dir


class TestModuleLoad(unittest.TestCase):
    """验证模块导入时的配置注入。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mt()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_scrapy_mt_injects_authorization_into_request_head(self):
        """模块加载时应把认证信息注入请求头。"""
        self.assertEqual(self.module.REQUEST_HEAD["Authorization"], "Bearer test-token")

    def test_load_scrapy_mt_sets_default_proxy_on_session(self):
        """模块加载时应给 ``session`` 预置代理。"""
        self.assertEqual(
            self.module.session.proxies,
            {
                "http": "http://127.0.0.1:7890",
                "https": "http://127.0.0.1:7890",
            },
        )

    def test_load_scrapy_mt_reads_query_time_from_config(self):
        """模块加载时应读取配置里的 ``query_time``。"""
        self.assertEqual(self.module.QUERY_TIME, "2026-03-25")


class TestMtHelpers(unittest.TestCase):
    """验证自动时间窗口和重试策略辅助函数。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mt()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_previous_day_returns_previous_day(self):
        """给定日期字符串时，应返回前一天。"""
        self.assertEqual(self.module.get_previous_day("2026-03-26"), "2026-03-25")

    def test_get_yesterday_date_str_supports_reference_date(self):
        """应支持用固定参考日期计算昨天，便于测试。"""
        self.assertEqual(
            self.module.get_yesterday_date_str(self.module.date(2026, 4, 26)),
            "2026-04-25",
        )

    def test_get_current_query_time_rereads_updated_config(self):
        """同进程内配置被回写后，应读到最新 ``query_time``。"""
        self.assertEqual(self.module.get_current_query_time(), "2026-03-25")

        self.module.update_json_config(self.module.CONFIG_PATH, "query_time", "2026-04-01")

        self.assertEqual(self.module.get_current_query_time(), "2026-04-01")

    def test_create_retry_strategy_prefers_allowed_methods(self):
        """urllib3 支持 ``allowed_methods`` 时应优先使用它。"""
        retry_instance = object()

        with patch.object(self.module, "Retry", return_value=retry_instance) as mock_retry:
            result = self.module.create_retry_strategy()

        self.assertIs(result, retry_instance)
        mock_retry.assert_called_once_with(
            allowed_methods=["POST", "GET"],
            total=15,
            status_forcelist=[502],
            backoff_factor=1,
        )

    def test_create_retry_strategy_falls_back_to_method_whitelist(self):
        """旧版 urllib3 不支持 ``allowed_methods`` 时应回退到 ``method_whitelist``。"""
        retry_instance = object()

        with patch.object(
            self.module,
            "Retry",
            side_effect=[TypeError("unsupported"), retry_instance],
        ) as mock_retry:
            result = self.module.create_retry_strategy()

        self.assertIs(result, retry_instance)
        self.assertEqual(
            mock_retry.call_args_list,
            [
                call(
                    allowed_methods=["POST", "GET"],
                    total=15,
                    status_forcelist=[502],
                    backoff_factor=1,
                ),
                call(
                    method_whitelist=["POST", "GET"],
                    total=15,
                    status_forcelist=[502],
                    backoff_factor=1,
                ),
            ],
        )


class TestPostMtResponse(unittest.TestCase):
    """验证单次 POST 请求逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mt()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_post_mt_response_returns_response_and_sets_utf8_encoding(self):
        """请求成功时应返回响应对象，并统一设置 UTF-8 编码。"""
        response = Mock(status_code=200)

        with patch.object(self.module.session, "post", return_value=response) as mock_post:
            result = self.module.post_mt_response("2024-01-01", "2024-01-31", 2)

        self.assertIs(result, response)
        self.assertEqual(response.encoding, "utf-8")
        mock_post.assert_called_once_with(
            "https://example.com/api/torrent/search",
            headers=self.module.REQUEST_HEAD,
            json={
                "categories": [],
                "mode": "movie",
                "pageNumber": 2,
                "pageSize": 100,
                "uploadDateStart": "2024-01-01 00:00:00",
                "uploadDateEnd": "2024-01-31 00:00:00",
                "visible": 0,
                "_sgin": "unit-sign",
                "_timestamp": "1700000000",
            },
            timeout=15,
        )

    def test_post_mt_response_raises_when_status_code_is_not_200(self):
        """请求返回非 200 状态码时应抛出异常。"""
        response = Mock(status_code=503)

        with patch.object(self.module.session, "post", return_value=response):
            with self.assertRaisesRegex(Exception, "503"):
                self.module.post_mt_response("2024-01-01", "2024-01-31", 5)

    def test_post_mt_response_propagates_request_exception(self):
        """底层请求异常时应直接抛出，交给重试装饰器处理。"""
        with patch.object(self.module.session, "post", side_effect=requests.Timeout("timed out")):
            with self.assertRaisesRegex(requests.Timeout, "timed out"):
                self.module.post_mt_response("2024-01-01", "2024-01-31", 1)


class TestParseMtResponse(unittest.TestCase):
    """验证 MT API 结果解析与落盘逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mt()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_mt_response_builds_filename_and_writes_json(self):
        """应按标题、简介、体积和 IMDb 编号拼出输出文件名。"""
        item_dict = {
            "name": "Movie/Title",
            "smallDescr": "Director's Cut",
            "imdb": "https://www.imdb.com/title/tt7654321/",
            "size": "1610612736",
        }

        with patch.object(self.module, "format_size", return_value="1.5 GB") as mock_format, patch.object(
            self.module,
            "normalize_release_title_for_filename",
            return_value="Normalized/Title",
        ) as mock_normalize, patch.object(
            self.module,
            "sanitize_filename",
            return_value="Safe Title",
        ) as mock_sanitize, patch.object(
            self.module,
            "write_dict_to_json",
        ) as mock_write:
            self.module.parse_mt_response([item_dict])

        mock_format.assert_called_once_with(1610612736)
        mock_normalize.assert_called_once_with(
            "Movie/Title[Director's Cut]",
            replace_pipe=False,
            replace_placeholder_dot=False,
        )
        mock_sanitize.assert_called_once_with("Normalized/Title")
        mock_write.assert_called_once_with(
            str(Path(self.module.OUTPUT_DIR) / "Safe Title(1.5GB)[tt7654321].ptmt"),
            item_dict,
        )

    def test_parse_mt_response_uses_empty_imdb_when_source_text_has_no_tt_id(self):
        """IMDb 字段里没有 ``tt`` 编号时，文件名里的 IMDb 段应留空。"""
        item_dict = {
            "name": "Movie Title",
            "smallDescr": "No IMDb",
            "imdb": "https://example.com/not-imdb",
            "size": "700",
        }

        with patch.object(self.module, "format_size", return_value="700 MB"), patch.object(
            self.module,
            "normalize_release_title_for_filename",
            return_value="Movie Title[No IMDb]",
        ), patch.object(
            self.module,
            "sanitize_filename",
            return_value="Movie Title[No IMDb]",
        ), patch.object(
            self.module,
            "write_dict_to_json",
        ) as mock_write:
            self.module.parse_mt_response([item_dict])

        mock_write.assert_called_once_with(
            str(Path(self.module.OUTPUT_DIR) / "Movie Title[No IMDb](700MB)[].ptmt"),
            item_dict,
        )

    def test_parse_mt_response_processes_every_item_in_list(self):
        """输入多条记录时，应逐条落盘而不是提前退出。"""
        items = [
            {
                "name": "Movie A",
                "smallDescr": "Desc A",
                "imdb": "tt1000001",
                "size": "1",
            },
            {
                "name": "Movie B",
                "smallDescr": "Desc B",
                "imdb": "tt1000002",
                "size": "2",
            },
        ]

        with patch.object(self.module, "format_size", side_effect=["1 KB", "2 KB"]), patch.object(
            self.module,
            "normalize_release_title_for_filename",
            side_effect=["Movie A[Desc A]", "Movie B[Desc B]"],
        ), patch.object(
            self.module,
            "sanitize_filename",
            side_effect=["Movie A[Desc A]", "Movie B[Desc B]"],
        ), patch.object(
            self.module,
            "write_dict_to_json",
        ) as mock_write:
            self.module.parse_mt_response(items)

        self.assertEqual(
            mock_write.call_args_list,
            [
                call(
                    str(Path(self.module.OUTPUT_DIR) / "Movie A[Desc A](1KB)[tt1000001].ptmt"),
                    items[0],
                ),
                call(
                    str(Path(self.module.OUTPUT_DIR) / "Movie B[Desc B](2KB)[tt1000002].ptmt"),
                    items[1],
                ),
            ],
        )

class TestScrapyMtMain(unittest.TestCase):
    """验证主抓取流程的编排逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_mt()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_mt_raises_when_first_response_is_not_success(self):
        """首次请求返回失败码时应立即报错。"""
        first_response = Mock()
        first_response.json.return_value = {"code": "1", "message": "FAIL", "data": {}}

        with patch.object(self.module, "get_current_query_time", return_value="2026-03-25"), patch.object(
            self.module,
            "get_yesterday_date_str",
            return_value="2026-04-25",
        ), patch.object(
            self.module,
            "post_mt_response",
            return_value=first_response,
        ), patch.object(
            self.module,
            "update_json_config",
        ) as mock_update:
            with self.assertRaisesRegex(Exception, "获取区间页数失败"):
                self.module.scrapy_mt()

        mock_update.assert_not_called()

    def test_scrapy_mt_raises_when_total_pages_hits_limit_100(self):
        """页数为 ``100`` 时应视为时间范围过大。"""
        first_response = Mock()
        first_response.json.return_value = {
            "code": "0",
            "message": "SUCCESS",
            "data": {"totalPages": "100"},
        }

        with patch.object(self.module, "get_current_query_time", return_value="2026-03-25"), patch.object(
            self.module,
            "get_yesterday_date_str",
            return_value="2026-04-25",
        ), patch.object(
            self.module,
            "post_mt_response",
            return_value=first_response,
        ), patch.object(
            self.module,
            "update_json_config",
        ) as mock_update:
            with self.assertRaisesRegex(Exception, "页数超过最大限制"):
                self.module.scrapy_mt()

        mock_update.assert_not_called()

    def test_scrapy_mt_fetches_each_page_and_updates_query_time_after_success(self):
        """成功拿到总页数后，应按页抓取并在末尾回写 ``query_time``。"""
        first_response = Mock()
        first_response.json.return_value = {
            "code": "0",
            "message": "SUCCESS",
            "data": {"totalPages": "2"},
        }
        page_one_response = Mock()
        page_one_response.json.return_value = {
            "data": {
                "data": [
                    {"name": "Movie A", "smallDescr": "Desc A", "imdb": "tt1", "size": "1"},
                ]
            }
        }
        page_two_response = Mock()
        page_two_response.json.return_value = {
            "data": {
                "data": [
                    {"name": "Movie B", "smallDescr": "Desc B", "imdb": "tt2", "size": "2"},
                ]
            }
        }

        with patch.object(
            self.module,
            "get_current_query_time",
            return_value="2026-03-25",
        ), patch.object(
            self.module,
            "get_yesterday_date_str",
            return_value="2026-04-25",
        ), patch.object(
            self.module,
            "post_mt_response",
            side_effect=[first_response, page_one_response, page_two_response],
        ) as mock_post, patch.object(self.module, "parse_mt_response") as mock_parse, patch.object(
            self.module,
            "update_json_config",
        ) as mock_update:
            self.module.scrapy_mt()

        self.assertEqual(
            mock_post.call_args_list,
            [
                call("2026-03-25", "2026-04-25", 100),
                call("2026-03-25", "2026-04-25", 1),
                call("2026-03-25", "2026-04-25", 2),
            ],
        )
        self.assertEqual(
            mock_parse.call_args_list,
            [
                call(
                    [
                        {"name": "Movie A", "smallDescr": "Desc A", "imdb": "tt1", "size": "1"},
                    ]
                ),
                call(
                    [
                        {"name": "Movie B", "smallDescr": "Desc B", "imdb": "tt2", "size": "2"},
                    ]
                ),
            ],
        )
        mock_update.assert_called_once_with(self.module.CONFIG_PATH, "query_time", "2026-04-25")

    def test_scrapy_mt_does_not_update_query_time_when_page_processing_raises(self):
        """分页处理中途出错时，不应回写 ``query_time``。"""
        first_response = Mock()
        first_response.json.return_value = {
            "code": "0",
            "message": "SUCCESS",
            "data": {"totalPages": "1"},
        }
        page_one_response = Mock()
        page_one_response.json.return_value = {
            "data": {
                "data": [
                    {"name": "Movie A", "smallDescr": "Desc A", "imdb": "tt1", "size": "1"},
                ]
            }
        }

        with patch.object(
            self.module,
            "get_current_query_time",
            return_value="2026-03-25",
        ), patch.object(
            self.module,
            "get_yesterday_date_str",
            return_value="2026-04-25",
        ), patch.object(
            self.module,
            "post_mt_response",
            side_effect=[first_response, page_one_response],
        ), patch.object(
            self.module,
            "parse_mt_response",
            side_effect=RuntimeError("boom"),
        ), patch.object(
            self.module,
            "update_json_config",
        ) as mock_update:
            with self.assertRaisesRegex(RuntimeError, "boom"):
                self.module.scrapy_mt()

        mock_update.assert_not_called()


if __name__ == "__main__":
    unittest.main()
