"""
针对 ``my_scripts.scrapy_ru`` 的单元测试。

这里不依赖真实配置文件，也不会发出真实网络请求。
目标是只验证抓取脚本的核心行为：
1. 单页请求的参数和异常处理。
2. 配置写回是否正确。
3. 帖子解析、文件写入、最大 topic_id 更新等抓取逻辑。
"""

import copy
import importlib.util
import json
import sys
import tempfile
import types
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from lxml import etree

requests.packages.urllib3.disable_warnings()

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_ru.py"
UPDATE_JSON_CONFIG_PATH = Path(__file__).resolve().parents[2] / "my_module" / "file_ops" / "update_json_config.py"


def load_scrapy_ru(config: dict | None = None):
    """
    在隔离环境中加载 ``scrapy_ru`` 模块。

    被测模块在 import 时就会读取配置并导入 ``my_module`` / ``retrying``，
    所以这里先注入假的依赖，再执行模块加载，避免测试依赖本地真实环境。
    """
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "scrapy_process": {"https://example.com/forum/viewforum.php?f=1": 0},
        "user_cookie": "cookie=value",
        "request_head": {"User-Agent": "unit-test"},
        "thread_number": 2,
        "torrent_path": temp_dir.name,
        "forum_url": "https://example.com/forum/",
    }
    if config:
        module_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: copy.deepcopy(module_config)
    fake_my_module.sanitize_filename = lambda name: name

    helper_spec = importlib.util.spec_from_file_location(
        f"update_json_config_test_{uuid.uuid4().hex}",
        UPDATE_JSON_CONFIG_PATH,
    )
    helper_module = importlib.util.module_from_spec(helper_spec)
    helper_spec.loader.exec_module(helper_module)
    fake_my_module.update_json_config = helper_module.update_json_config

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = lambda *args, **kwargs: (lambda func: func)

    spec = importlib.util.spec_from_file_location(f"scrapy_ru_test_{uuid.uuid4().hex}", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    # 用假的依赖加载目标模块，确保测试过程不读取真实配置、不走真实重试装饰器。
    with patch.dict(sys.modules, {"my_module": fake_my_module, "retrying": fake_retrying}):
        spec.loader.exec_module(module)

    return module, temp_dir


def build_topic_row(topic_id: str, title: str, size_text: str) -> str:
    """构造一条最小可用的帖子行 HTML。"""
    return f"""
    <tr class="hl-tr">
      <td class="vf-col-t-title tt">
        <a class="torTopic bold tt-text" href="viewtopic.php?t={topic_id}">{title}</a>
      </td>
      <td class="vf-col-tor tCenter med nowrap">
        <a class="small f-dl dl-stub" href="dl.php?t={topic_id}">{size_text}</a>
      </td>
    </tr>
    """


def build_page_html(*rows: str) -> str:
    """把若干帖子行拼成单页 HTML。"""
    return f"<table>{''.join(rows)}</table>"


def build_next_page_link(href: str) -> str:
    """构造下一页链接，匹配被测脚本当前使用的 XPath。"""
    return f'<a class="pg" href="{href}">След.</a>'


class TestGetPage(unittest.TestCase):
    """验证单页请求函数的入参与异常行为。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ru()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_page_returns_response_and_appends_sort(self):
        """请求成功时应返回响应对象，并自动拼接 ``&sort=2``。"""
        response = Mock(status_code=200, text="ok")

        with patch.object(self.module.requests, "get", return_value=response) as mock_get:
            result = self.module.get_page("https://example.com/forum/viewforum.php?f=1")

        self.assertIs(result, response)
        mock_get.assert_called_once_with(
            url="https://example.com/forum/viewforum.php?f=1&sort=2",
            headers=self.module.REQUEST_HEAD,
            timeout=10,
            verify=False,
            allow_redirects=True,
        )

    def test_get_page_raises_when_status_code_is_not_200(self):
        """请求返回非 200 状态码时应抛出异常。"""
        response = Mock(status_code=503, text="busy")

        with patch.object(self.module.requests, "get", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "状态码：503"):
                self.module.get_page("https://example.com/forum/viewforum.php?f=1")

    def test_get_page_propagates_request_exception(self):
        """底层请求直接抛异常时，应透传给调用方处理。"""
        with patch.object(self.module.requests, "get", side_effect=requests.RequestException("network down")):
            with self.assertRaisesRegex(requests.RequestException, "network down"):
                self.module.get_page("https://example.com/forum/viewforum.php?f=1")


class TestParseTopicRow(unittest.TestCase):
    """验证单条帖子行的解析逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ru()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_topic_row_returns_structured_data(self):
        """结构完整的帖子行应被解析成标题、ID、链接和大小信息。"""
        html = build_page_html(build_topic_row("250", "Valid Topic", "2 MB"))
        row = etree.HTML(html).xpath('//tr[@class="hl-tr"]')[0]

        result = self.module.parse_topic_row(row)

        self.assertEqual(
            result,
            {
                "title_text": "Valid Topic",
                "topic_id": "250",
                "topic_link": "https://example.com/forum/viewtopic.php?t=250",
                "size_text": "2 MB",
                "download_link": "https://example.com/forum/dl.php?t=250",
            },
        )

    def test_parse_topic_row_returns_none_when_title_link_is_missing(self):
        """缺少标题链接时，当前帖子行应被判定为无效。"""
        html = build_page_html(
            """
            <tr class="hl-tr">
              <td class="vf-col-t-title tt"></td>
              <td class="vf-col-tor tCenter med nowrap">
                <a class="small f-dl dl-stub" href="dl.php?t=100">1 MB</a>
              </td>
            </tr>
            """
        )
        row = etree.HTML(html).xpath('//tr[@class="hl-tr"]')[0]

        self.assertIsNone(self.module.parse_topic_row(row))

    def test_parse_topic_row_returns_none_when_download_link_is_missing(self):
        """缺少下载链接时，当前帖子行应被判定为无效。"""
        html = build_page_html(
            """
            <tr class="hl-tr">
              <td class="vf-col-t-title tt">
                <a class="torTopic bold tt-text" href="viewtopic.php?t=100">Broken Topic</a>
              </td>
              <td class="vf-col-tor tCenter med nowrap"></td>
            </tr>
            """
        )
        row = etree.HTML(html).xpath('//tr[@class="hl-tr"]')[0]

        self.assertIsNone(self.module.parse_topic_row(row))


class TestBuildOutputFilename(unittest.TestCase):
    """验证输出文件名生成逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ru()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_build_output_filename_keeps_normal_title(self):
        """普通标题应直接拼出文件名，并保留 topic_id 与大小信息。"""
        result = self.module.build_output_filename("Valid Topic", "250", "2 MB")

        self.assertEqual(result, "Valid Topic[250][2 MB].txt")

    def test_build_output_filename_truncates_long_title(self):
        """标题超过长度上限时，应先截断再生成文件名。"""
        long_title = "A" * 230
        truncated_title = "A" * 228

        result = self.module.build_output_filename(long_title, "250", "2 MB")

        self.assertEqual(result, f"{truncated_title}[250][2 MB].txt")

    def test_build_output_filename_replaces_path_separators_before_sanitize(self):
        """路径分隔符应先替换成全角竖线，再交给 ``sanitize_filename`` 处理。"""
        with patch.object(self.module, "sanitize_filename", return_value="safe-name.txt") as mock_sanitize:
            result = self.module.build_output_filename(r"Title/A\B", "250", "2 MB")

        self.assertEqual(result, "safe-name.txt")
        mock_sanitize.assert_called_once_with("Title｜A｜B[250][2 MB].txt")


class TestGetNextPageUrl(unittest.TestCase):
    """验证下一页链接提取逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ru()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_next_page_url_returns_full_url_when_link_exists(self):
        """存在“下一页”链接时，应返回拼好的完整 URL。"""
        tree = etree.HTML(build_page_html(build_topic_row("250", "Topic", "2 MB")) + build_next_page_link("viewforum.php?f=1&start=50"))

        result = self.module.get_next_page_url(tree)

        self.assertEqual(result, "https://example.com/forum/viewforum.php?f=1&start=50")

    def test_get_next_page_url_returns_none_when_link_is_missing(self):
        """不存在“下一页”链接时，应返回 ``None``。"""
        tree = etree.HTML(build_page_html(build_topic_row("250", "Topic", "2 MB")))

        self.assertIsNone(self.module.get_next_page_url(tree))


class TestWriteTopicFile(unittest.TestCase):
    """验证帖子文件写入逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ru()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_write_topic_file_creates_target_file(self):
        """写入帖子文件时，应在目标目录中创建对应文本文件。"""
        self.module.write_topic_file("topic.txt", "line1\nline2")

        output_path = Path(self.temp_dir.name) / "topic.txt"
        self.assertTrue(output_path.exists())
        self.assertEqual(output_path.read_text(encoding="utf-8"), "line1\nline2")

    def test_write_topic_file_writes_utf8_content(self):
        """中文和俄文内容应按 UTF-8 正常写入和读取。"""
        self.module.write_topic_file("utf8.txt", "测试\nПривет")

        output_path = Path(self.temp_dir.name) / "utf8.txt"
        self.assertEqual(output_path.read_text(encoding="utf-8"), "测试\nПривет")


class TestProcessPageRows(unittest.TestCase):
    """验证单页帖子行处理逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ru()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_process_page_rows_updates_max_id_and_writes_files(self):
        """遇到新帖子时，应写文件并返回本页更新后的最大 ID。"""
        row_elements = etree.HTML(
            build_page_html(
                build_topic_row("100", "Topic One", "1 MB"),
                build_topic_row("250", "Topic Two", "2 MB"),
            )
        ).xpath('//tr[@class="hl-tr"]')

        stop, max_id = self.module.process_page_rows(row_elements, stop_id=0, current_max_id=None)

        self.assertFalse(stop)
        self.assertEqual(max_id, "250")
        self.assertTrue((Path(self.temp_dir.name) / "Topic One[100][1 MB].txt").exists())
        self.assertTrue((Path(self.temp_dir.name) / "Topic Two[250][2 MB].txt").exists())

    def test_process_page_rows_skips_invalid_rows(self):
        """缺标题或缺下载链接的行应被跳过，不影响有效行处理。"""
        row_elements = etree.HTML(
            build_page_html(
                """
                <tr class="hl-tr">
                  <td class="vf-col-t-title tt"></td>
                  <td class="vf-col-tor tCenter med nowrap">
                    <a class="small f-dl dl-stub" href="dl.php?t=100">1 MB</a>
                  </td>
                </tr>
                """,
                """
                <tr class="hl-tr">
                  <td class="vf-col-t-title tt">
                    <a class="torTopic bold tt-text" href="viewtopic.php?t=150">Broken Topic</a>
                  </td>
                  <td class="vf-col-tor tCenter med nowrap"></td>
                </tr>
                """,
                build_topic_row("250", "Valid Topic", "2 MB"),
            )
        ).xpath('//tr[@class="hl-tr"]')

        stop, max_id = self.module.process_page_rows(row_elements, stop_id=0, current_max_id=None)

        self.assertFalse(stop)
        self.assertEqual(max_id, "250")
        self.assertEqual([path.name for path in Path(self.temp_dir.name).iterdir()], ["Valid Topic[250][2 MB].txt"])

    def test_process_page_rows_sets_stop_when_old_topic_is_found(self):
        """遇到旧帖子时应返回 stop=True，并保留之前已经算出的最大 ID。"""
        row_elements = etree.HTML(
            build_page_html(
                build_topic_row("260", "Topic New", "1 MB"),
                build_topic_row("150", "Topic Old", "2 MB"),
            )
        ).xpath('//tr[@class="hl-tr"]')

        stop, max_id = self.module.process_page_rows(row_elements, stop_id=200, current_max_id="300")

        self.assertTrue(stop)
        self.assertEqual(max_id, "300")
        self.assertTrue((Path(self.temp_dir.name) / "Topic New[260][1 MB].txt").exists())
        self.assertFalse((Path(self.temp_dir.name) / "Topic Old[150][2 MB].txt").exists())


class TestUpdateJsonConfig(unittest.TestCase):
    """验证配置文件写回逻辑。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ru()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_update_json_config_updates_target_key(self):
        """更新配置时只应改目标栏目，不应影响其它栏目。"""
        config_path = Path(self.temp_dir.name) / "scrapy_ru.json"
        config_data = {
            "scrapy_process": {
                "https://example.com/forum/viewforum.php?f=1": "100",
                "https://example.com/forum/viewforum.php?f=2": "200",
            }
        }
        config_path.write_text(json.dumps(config_data, ensure_ascii=False), encoding="utf-8")

        self.module.update_json_config(
            str(config_path),
            ["scrapy_process", "https://example.com/forum/viewforum.php?f=1"],
            "300",
        )

        updated = json.loads(config_path.read_text(encoding="utf-8"))
        self.assertEqual(updated["scrapy_process"]["https://example.com/forum/viewforum.php?f=1"], "300")
        self.assertEqual(updated["scrapy_process"]["https://example.com/forum/viewforum.php?f=2"], "200")
        self.assertFalse(Path(f"{config_path}.tmp").exists())

    def test_update_json_config_keeps_other_keys_after_concurrent_updates(self):
        """并发更新不同栏目时，最终配置应同时保留各自的最新值。"""
        config_path = Path(self.temp_dir.name) / "scrapy_ru.json"
        config_data = {
            "scrapy_process": {
                "https://example.com/forum/viewforum.php?f=1": "100",
                "https://example.com/forum/viewforum.php?f=2": "200",
            }
        }
        config_path.write_text(json.dumps(config_data, ensure_ascii=False), encoding="utf-8")

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    self.module.update_json_config,
                    str(config_path),
                    ["scrapy_process", "https://example.com/forum/viewforum.php?f=1"],
                    "300",
                ),
                executor.submit(
                    self.module.update_json_config,
                    str(config_path),
                    ["scrapy_process", "https://example.com/forum/viewforum.php?f=2"],
                    "400",
                ),
            ]
            for future in futures:
                future.result()

        updated = json.loads(config_path.read_text(encoding="utf-8"))
        self.assertEqual(updated["scrapy_process"]["https://example.com/forum/viewforum.php?f=1"], "300")
        self.assertEqual(updated["scrapy_process"]["https://example.com/forum/viewforum.php?f=2"], "400")
        self.assertFalse(Path(f"{config_path}.tmp").exists())

    def test_update_json_config_supports_json_list_values(self):
        """配置更新函数应允许把列表直接写入 JSON。"""
        config_path = Path(self.temp_dir.name) / "scrapy_ru.json"
        config_data = {
            "scrapy_process": {
                "https://example.com/forum/viewforum.php?f=1": "100",
            }
        }
        config_path.write_text(json.dumps(config_data, ensure_ascii=False), encoding="utf-8")

        self.module.update_json_config(
            str(config_path),
            ["scrapy_process", "https://example.com/forum/viewforum.php?f=1"],
            ["300", "301"],
        )

        updated = json.loads(config_path.read_text(encoding="utf-8"))
        self.assertEqual(updated["scrapy_process"]["https://example.com/forum/viewforum.php?f=1"], ["300", "301"])
        self.assertFalse(Path(f"{config_path}.tmp").exists())


class TestScripy(unittest.TestCase):
    """验证主抓取函数的解析、落盘和配置更新行为。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ru()
        self.base_url = "https://example.com/forum/viewforum.php?f=1"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scripy_writes_files_and_updates_new_max_id(self):
        """正常解析页面时应写出文件，并把本次最大 topic_id 写回配置。"""
        html = build_page_html(
            build_topic_row("100", "Topic One", "1 MB"),
            build_topic_row("250", "Topic Two", "2 MB"),
        )
        response = Mock(text=html)

        with patch.object(self.module, "get_page", return_value=response), patch.object(
                self.module, "update_json_config"
        ) as mock_update:
            self.module.scripy(self.base_url)

        # 既要确认文件被写出来，也要确认本次栏目内最大 topic_id 被写回配置。
        topic_one = Path(self.temp_dir.name) / "Topic One[100][1 MB].txt"
        topic_two = Path(self.temp_dir.name) / "Topic Two[250][2 MB].txt"
        self.assertTrue(topic_one.exists())
        self.assertTrue(topic_two.exists())
        self.assertEqual(
            topic_two.read_text(encoding="utf-8"),
            "https://example.com/forum/viewtopic.php?t=250\nhttps://example.com/forum/dl.php?t=250",
        )
        mock_update.assert_called_once_with(self.module.CONFIG_PATH, ["scrapy_process", self.base_url], "250")

    def test_scripy_raises_when_topic_rows_are_missing(self):
        """页面中找不到帖子行时应抛出异常，而不是静默返回。"""
        response = Mock(text="<html><body>empty</body></html>")

        with patch.object(self.module, "get_page", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "未找到种子行"):
                self.module.scripy(self.base_url)

    def test_scripy_skips_old_topics_without_updating_config(self):
        """页面里的帖子都早于 stop_id 时，应跳过写文件和配置更新。"""
        self.module.CONFIG["scrapy_process"][self.base_url] = 500
        html = build_page_html(build_topic_row("100", "Topic One", "1 MB"))
        response = Mock(text=html)

        with patch.object(self.module, "get_page", return_value=response), patch.object(
                self.module, "update_json_config"
        ) as mock_update:
            self.module.scripy(self.base_url)

        # 小于 stop_id 的旧帖子应被跳过，因此既不落盘，也不更新配置。
        self.assertEqual(list(Path(self.temp_dir.name).iterdir()), [])
        mock_update.assert_not_called()

    def test_scripy_follows_next_page_and_processes_both_pages(self):
        """存在下一页链接时应继续抓取，并处理后续页面的帖子。"""
        next_url = "https://example.com/forum/viewforum.php?f=1&start=50"
        page_one = Mock(
            text=build_page_html(build_topic_row("300", "Topic One", "1 MB"))
            + build_next_page_link("viewforum.php?f=1&start=50")
        )
        page_two = Mock(text=build_page_html(build_topic_row("250", "Topic Two", "2 MB")))

        with patch.object(self.module, "get_page", side_effect=[page_one, page_two]) as mock_get_page, patch.object(
                self.module, "update_json_config"
        ) as mock_update:
            self.module.scripy(self.base_url)

        self.assertEqual(mock_get_page.call_args_list, [((self.base_url,),), ((next_url,),)])
        self.assertTrue((Path(self.temp_dir.name) / "Topic One[300][1 MB].txt").exists())
        self.assertTrue((Path(self.temp_dir.name) / "Topic Two[250][2 MB].txt").exists())
        mock_update.assert_called_once_with(self.module.CONFIG_PATH, ["scrapy_process", self.base_url], "300")

    def test_scripy_stops_pagination_when_old_topic_is_found(self):
        """当前页已经遇到旧帖子时，不应继续请求下一页。"""
        self.module.CONFIG["scrapy_process"][self.base_url] = 500
        response = Mock(
            text=build_page_html(build_topic_row("100", "Topic One", "1 MB"))
            + build_next_page_link("viewforum.php?f=1&start=50")
        )

        with patch.object(self.module, "get_page", return_value=response) as mock_get_page, patch.object(
                self.module, "update_json_config"
        ) as mock_update:
            self.module.scripy(self.base_url)

        self.assertEqual(mock_get_page.call_count, 1)
        self.assertEqual(list(Path(self.temp_dir.name).iterdir()), [])
        mock_update.assert_not_called()

    def test_scripy_does_not_update_config_when_next_page_fails(self):
        """后续页失败时，前页文件可以保留，但配置不能提前推进。"""
        page_one = Mock(
            text=build_page_html(build_topic_row("300", "Topic One", "1 MB"))
            + build_next_page_link("viewforum.php?f=1&start=50")
        )

        with patch.object(self.module, "get_page", side_effect=[page_one, RuntimeError("boom")]), patch.object(
                self.module, "update_json_config"
        ) as mock_update:
            with self.assertRaisesRegex(RuntimeError, "boom"):
                self.module.scripy(self.base_url)

        # 即使后续页失败，前一页已经写出的文件仍然存在；关键是不能推进配置。
        self.assertTrue((Path(self.temp_dir.name) / "Topic One[300][1 MB].txt").exists())
        mock_update.assert_not_called()

    def test_scripy_keeps_original_group_key_across_pages(self):
        """翻页后仍应沿用原栏目 key，而不是把分页 URL 当成新栏目。"""
        self.module.CONFIG["scrapy_process"][self.base_url] = 200
        page_one = Mock(
            text=build_page_html(build_topic_row("300", "Topic One", "1 MB"))
            + build_next_page_link("viewforum.php?f=1&start=50")
        )
        page_two = Mock(text=build_page_html(build_topic_row("150", "Topic Two", "2 MB")))

        with patch.object(self.module, "get_page", side_effect=[page_one, page_two]), patch.object(
                self.module, "update_json_config"
        ) as mock_update:
            self.module.scripy(self.base_url)

        # 第二页仍应沿用原栏目的 stop_id=200，因此旧帖子不应被写出。
        self.assertTrue((Path(self.temp_dir.name) / "Topic One[300][1 MB].txt").exists())
        self.assertFalse((Path(self.temp_dir.name) / "Topic Two[150][2 MB].txt").exists())
        mock_update.assert_called_once_with(self.module.CONFIG_PATH, ["scrapy_process", self.base_url], "300")

    def test_scripy_skips_row_without_title_link(self):
        """缺少标题链接的帖子行应被跳过，不应写出无效文件。"""
        html = build_page_html(
            """
            <tr class="hl-tr">
              <td class="vf-col-t-title tt"></td>
              <td class="vf-col-tor tCenter med nowrap">
                <a class="small f-dl dl-stub" href="dl.php?t=100">1 MB</a>
              </td>
            </tr>
            """,
            build_topic_row("250", "Valid Topic", "2 MB"),
        )
        response = Mock(text=html)

        with patch.object(self.module, "get_page", return_value=response), patch.object(
                self.module, "update_json_config"
        ) as mock_update:
            self.module.scripy(self.base_url)

        self.assertEqual([path.name for path in Path(self.temp_dir.name).iterdir()], ["Valid Topic[250][2 MB].txt"])
        mock_update.assert_called_once_with(self.module.CONFIG_PATH, ["scrapy_process", self.base_url], "250")

    def test_scripy_skips_row_without_download_link(self):
        """缺少下载链接的帖子行应被跳过，只处理结构完整的帖子。"""
        html = build_page_html(
            """
            <tr class="hl-tr">
              <td class="vf-col-t-title tt">
                <a class="torTopic bold tt-text" href="viewtopic.php?t=100">Broken Topic</a>
              </td>
              <td class="vf-col-tor tCenter med nowrap"></td>
            </tr>
            """,
            build_topic_row("250", "Valid Topic", "2 MB"),
        )
        response = Mock(text=html)

        with patch.object(self.module, "get_page", return_value=response), patch.object(
                self.module, "update_json_config"
        ) as mock_update:
            self.module.scripy(self.base_url)

        self.assertEqual([path.name for path in Path(self.temp_dir.name).iterdir()], ["Valid Topic[250][2 MB].txt"])
        mock_update.assert_called_once_with(self.module.CONFIG_PATH, ["scrapy_process", self.base_url], "250")

    def test_scripy_truncates_long_title_before_creating_filename(self):
        """标题过长时应先截断，再生成输出文件名。"""
        long_title = "A" * 230
        truncated_title = "A" * 228
        html = build_page_html(build_topic_row("250", long_title, "2 MB"))
        response = Mock(text=html)

        with patch.object(self.module, "get_page", return_value=response), patch.object(
                self.module, "update_json_config"
        ):
            self.module.scripy(self.base_url)

        self.assertTrue((Path(self.temp_dir.name) / f"{truncated_title}[250][2 MB].txt").exists())

    def test_scripy_sanitizes_filename_after_replacing_path_separators(self):
        """生成文件名时应先替换路径分隔符，再调用 ``sanitize_filename``。"""
        html = build_page_html(build_topic_row("250", r"Title/A\B", "2 MB"))
        response = Mock(text=html)

        with patch.object(self.module, "get_page", return_value=response), patch.object(
                self.module, "sanitize_filename", return_value="safe-name.txt"
        ) as mock_sanitize, patch.object(self.module, "update_json_config"):
            self.module.scripy(self.base_url)

        mock_sanitize.assert_called_once_with("Title｜A｜B[250][2 MB].txt")
        self.assertTrue((Path(self.temp_dir.name) / "safe-name.txt").exists())

    def test_scripy_writes_utf8_file_for_non_ascii_title(self):
        """中文和俄文标题应能正常写入文件名，文件内容也应可按 UTF-8 读取。"""
        html = build_page_html(build_topic_row("250", "测试标题 Привет", "2 MB"))
        response = Mock(text=html)

        with patch.object(self.module, "get_page", return_value=response), patch.object(
                self.module, "update_json_config"
        ):
            self.module.scripy(self.base_url)

        output_path = Path(self.temp_dir.name) / "测试标题 Привет[250][2 MB].txt"
        self.assertTrue(output_path.exists())
        self.assertEqual(
            output_path.read_text(encoding="utf-8"),
            "https://example.com/forum/viewtopic.php?t=250\nhttps://example.com/forum/dl.php?t=250",
        )

    def test_scripy_processes_new_topics_before_old_topic_on_same_page(self):
        """同一页先出现新帖子、后出现旧帖子时，应写出新帖子并停止继续翻页。"""
        self.module.CONFIG["scrapy_process"][self.base_url] = 200
        html = build_page_html(
            build_topic_row("300", "Topic New 1", "1 MB"),
            build_topic_row("250", "Topic New 2", "2 MB"),
            build_topic_row("150", "Topic Old", "3 MB"),
        ) + build_next_page_link("viewforum.php?f=1&start=50")
        response = Mock(text=html)

        with patch.object(self.module, "get_page", return_value=response) as mock_get_page, patch.object(
                self.module, "update_json_config"
        ) as mock_update:
            self.module.scripy(self.base_url)

        self.assertEqual(mock_get_page.call_count, 1)
        self.assertTrue((Path(self.temp_dir.name) / "Topic New 1[300][1 MB].txt").exists())
        self.assertTrue((Path(self.temp_dir.name) / "Topic New 2[250][2 MB].txt").exists())
        self.assertFalse((Path(self.temp_dir.name) / "Topic Old[150][3 MB].txt").exists())
        mock_update.assert_called_once_with(self.module.CONFIG_PATH, ["scrapy_process", self.base_url], "300")

    def test_scripy_updates_config_once_with_max_id_across_all_pages(self):
        """多页都成功时，应只写一次配置，并取整个栏目链路中的最大 ID。"""
        page_one = Mock(
            text=build_page_html(build_topic_row("250", "Topic One", "1 MB"))
            + build_next_page_link("viewforum.php?f=1&start=50")
        )
        page_two = Mock(text=build_page_html(build_topic_row("300", "Topic Two", "2 MB")))

        with patch.object(self.module, "get_page", side_effect=[page_one, page_two]), patch.object(
                self.module, "update_json_config"
        ) as mock_update:
            self.module.scripy(self.base_url)

        self.assertTrue((Path(self.temp_dir.name) / "Topic One[250][1 MB].txt").exists())
        self.assertTrue((Path(self.temp_dir.name) / "Topic Two[300][2 MB].txt").exists())
        mock_update.assert_called_once_with(self.module.CONFIG_PATH, ["scrapy_process", self.base_url], "300")


class TestScrapyRuEntrypoint(unittest.TestCase):
    """验证多栏目入口函数的调度和异常兜底行为。"""

    def setUp(self):
        self.module, self.temp_dir = load_scrapy_ru()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_ru_continues_when_one_group_fails(self):
        """一个栏目失败时，入口函数仍应继续处理其它栏目。"""
        urls = ["https://example.com/forum/a", "https://example.com/forum/b"]

        def fake_scripy(url: str):
            if url.endswith("/b"):
                raise RuntimeError("boom")

        with patch.object(self.module, "SCRAPY_GROUP", urls), patch.object(
                self.module, "THREAD_NUMBER", 1
        ), patch.object(self.module, "scripy", side_effect=fake_scripy) as mock_scripy, patch.object(
                self.module.logger, "info"
        ) as mock_info, patch.object(self.module.logger, "error") as mock_error:
            self.module.scrapy_ru()

        self.assertEqual(mock_scripy.call_args_list, [((urls[0],),), ((urls[1],),)])
        mock_info.assert_called_once_with(f"抓取完成：{urls[0]}")
        mock_error.assert_called_once()
        self.assertIn(urls[1], mock_error.call_args[0][0])

    def test_scrapy_ru_logs_success_for_each_group_when_all_succeed(self):
        """全部栏目都成功时，应逐个记录成功日志且不记录错误日志。"""
        urls = ["https://example.com/forum/a", "https://example.com/forum/b"]

        with patch.object(self.module, "SCRAPY_GROUP", urls), patch.object(
                self.module, "THREAD_NUMBER", 1
        ), patch.object(self.module, "scripy") as mock_scripy, patch.object(
                self.module.logger, "info"
        ) as mock_info, patch.object(self.module.logger, "error") as mock_error:
            self.module.scrapy_ru()

        self.assertEqual(mock_scripy.call_args_list, [((urls[0],),), ((urls[1],),)])
        self.assertEqual(
            [call.args[0] for call in mock_info.call_args_list],
            [f"抓取完成：{urls[0]}", f"抓取完成：{urls[1]}"],
        )
        mock_error.assert_not_called()


if __name__ == "__main__":
    unittest.main()
