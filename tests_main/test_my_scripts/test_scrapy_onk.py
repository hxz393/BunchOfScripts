"""
针对 ``my_scripts.scrapy_onk`` 的单元测试。
"""

import copy
import datetime
import importlib.util
import json
import re
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, call, patch

import requests

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_onk.py"


class FakeSession:
    def __init__(self):
        self.proxies = {}
        self.mount_calls = []

    def mount(self, prefix: str, adapter):
        self.mount_calls.append((prefix, adapter))

    def get(self, *args, **kwargs):  # pragma: no cover
        raise AssertionError("session.get should be patched in tests")


class MiniRedis:
    def __init__(self):
        self.values = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value
        return True

    def delete(self, *keys):
        deleted = 0
        for key in keys:
            if key in self.values:
                del self.values[key]
                deleted += 1
        return deleted

    def sadd(self, key, *values):
        bucket = self.values.setdefault(key, set())
        added = 0
        for value in values:
            if value not in bucket:
                bucket.add(value)
                added += 1
        return added

    def rpush(self, key, *values):
        bucket = self.values.setdefault(key, [])
        bucket.extend(values)
        return len(bucket)

    def llen(self, key):
        return len(self.values.get(key, []))

    def rpoplpush(self, source, destination):
        source_bucket = self.values.get(source, [])
        if not source_bucket:
            return None
        value = source_bucket.pop()
        self.values.setdefault(destination, []).insert(0, value)
        return value

    def lrem(self, key, count, value):
        bucket = self.values.get(key, [])
        if not bucket:
            return 0
        removed = 0
        result = []
        limit = len(bucket) if count == 0 else abs(count)
        for item in bucket:
            if item == value and removed < limit:
                removed += 1
                continue
            result.append(item)
        self.values[key] = result
        return removed


def fake_retry(*args, **kwargs):
    max_attempts = kwargs.get("stop_max_attempt_number", 1)

    def decorator(func):
        def wrapper(*func_args, **func_kwargs):
            last_exception = None
            for _ in range(max_attempts):
                try:
                    return func(*func_args, **func_kwargs)
                except Exception as exc:  # pragma: no cover
                    last_exception = exc
            raise last_exception

        return wrapper

    return decorator


def fake_extract_drive_urls(html_text: str) -> list[str]:
    results = []
    seen = set()
    for url in re.findall(r'https://drive\.google\.com/[^"\'>\s]+', html_text):
        if url in seen:
            continue
        seen.add(url)
        results.append(url)
    return results


def fake_recover_processing_queue(redis_client, *, processing_key, pending_key, logger, queue_label):
    recovered = 0
    while True:
        payload = redis_client.rpoplpush(processing_key, pending_key)
        if not payload:
            return recovered
        recovered += 1


def fake_drain_queue(
    redis_client,
    *,
    pending_key,
    processing_key,
    worker,
    deserialize,
    keep_failed_in_processing=False,
    failed_key=None,
    **kwargs,
):
    while True:
        payload = redis_client.rpoplpush(pending_key, processing_key)
        if not payload:
            return
        info = deserialize(payload)
        try:
            worker(info)
            redis_client.lrem(processing_key, 1, payload)
        except Exception:
            if not keep_failed_in_processing:
                redis_client.lrem(processing_key, 1, payload)
                if failed_key is not None:
                    redis_client.rpush(failed_key, payload)


def load_scrapy_onk(config: dict | None = None):
    temp_dir = tempfile.TemporaryDirectory()
    module_config = {
        "group_dict": {"电影": 10},
        "output_dir": str(Path(temp_dir.name) / "downloads"),
        "onk_url": "https://onk.example",
        "onk_cookie": "cookie=value",
        "request_head": {"User-Agent": "unit-test"},
        "end_time": "2026-03-25",
        "thread_number": 10,
    }
    if config:
        module_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.normalize_release_title_for_filename = lambda title: title.replace("/", "｜")
    fake_my_module.read_file_to_list = lambda path: Path(path).read_text(encoding="utf-8").splitlines()
    fake_my_module.read_json_to_dict = lambda _path: copy.deepcopy(module_config)
    fake_my_module.sanitize_filename = lambda name: name.replace(":", " ")
    fake_my_module.update_json_config = lambda _path, key, value: module_config.__setitem__(key, value)

    def fake_write_list_to_file(path: str, items: list[str]) -> None:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("\n".join(items), encoding="utf-8")

    fake_my_module.write_list_to_file = fake_write_list_to_file

    fake_retrying = types.ModuleType("retrying")
    fake_retrying.retry = fake_retry

    fake_gd_downloader = types.ModuleType("scrapy_gd_downloader")
    fake_gd_downloader.extract_drive_urls = fake_extract_drive_urls
    fake_gd_downloader.download_gd_url = lambda _url: (_ for _ in ()).throw(
        AssertionError("download_gd_url should be patched in tests")
    )

    fake_redis_module = types.ModuleType("scrapy_redis")
    fake_redis_module.get_redis_client = lambda: MiniRedis()
    fake_redis_module.serialize_payload = lambda payload: json.dumps(payload, ensure_ascii=False, sort_keys=True)
    fake_redis_module.deserialize_payload = json.loads
    fake_redis_module.recover_processing_queue = fake_recover_processing_queue
    fake_redis_module.drain_queue = fake_drain_queue

    spec = importlib.util.spec_from_file_location(f"scrapy_onk_test_{uuid.uuid4().hex}", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "my_module": fake_my_module,
            "retrying": fake_retrying,
            "scrapy_gd_downloader": fake_gd_downloader,
            "scrapy_redis": fake_redis_module,
        },
    ), patch.object(requests, "Session", FakeSession):
        spec.loader.exec_module(module)

    return module, temp_dir


def build_thread_html(
    *,
    title: str = "帖子标题",
    href: str = "/threads/thread-1",
    label: str | None = "NZB",
    imdb_href: str | None = "https://www.imdb.com/title/tt1234567/",
    datetime_attr: str | None = "2026-01-16T15:51:20+0000",
) -> str:
    label_html = f'<a class="labelLink"><span>{label}</span></a>' if label is not None else ""
    imdb_html = f'<span class="imdb"><a href="{imdb_href}">IMDb</a></span>' if imdb_href is not None else ""
    time_html = '<div class="structItem-startDate"></div>'
    if datetime_attr is not None:
        time_html = f'<div class="structItem-startDate"><time class="u-dt" datetime="{datetime_attr}"></time></div>'
    return (
        f'<div class="structItem structItem--thread">{label_html}'
        f'<div class="structItem-title"><a href="{href}">{title}</a></div>{imdb_html}{time_html}</div>'
    )


def make_forum_response(*items: str):
    html = "".join(items)
    return Mock(text=html, content=html.encode("utf-8"))


class TestModuleLoad(unittest.TestCase):
    def setUp(self):
        self.module, self.temp_dir = load_scrapy_onk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_scrapy_onk_injects_cookie_and_initializes_session(self):
        self.assertEqual(self.module.REQUEST_HEAD["Cookie"], "cookie=value")
        self.assertEqual(self.module.THREAD_NUMBER, 10)
        self.assertEqual(self.module.session.proxies, {})
        self.assertEqual([prefix for prefix, _adapter in self.module.session.mount_calls], ["http://", "https://"])

    def test_build_session_uses_thread_count_for_pool_size(self):
        with patch.object(self.module.requests, "Session", FakeSession):
            session = self.module.build_session(23)

        self.assertEqual(session.proxies, {})
        self.assertEqual([prefix for prefix, _adapter in session.mount_calls], ["http://", "https://"])
        self.assertEqual(session.mount_calls[0][1]._pool_connections, 23)
        self.assertEqual(session.mount_calls[0][1]._pool_maxsize, 23)


class TestEndTimeHelpers(unittest.TestCase):
    def setUp(self):
        self.module, self.temp_dir = load_scrapy_onk()
        self.redis_client = MiniRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_current_end_time_and_yesterday_helpers(self):
        self.assertEqual(self.module.get_current_end_time(), "2026-03-25")
        self.module.update_json_config(self.module.CONFIG_PATH, "end_time", "2026-04-01")
        self.assertEqual(self.module.get_current_end_time(), "2026-04-01")
        self.assertEqual(self.module.get_yesterday_date_str(datetime.date(2026, 4, 26)), "2026-04-25")

    def test_finalize_onk_run_updates_only_when_scan_and_queues_are_finished(self):
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")
        with patch.object(self.module, "get_yesterday_date_str", return_value="2026-04-25"), patch.object(
            self.module, "update_json_config"
        ) as mock_update:
            self.module.finalize_onk_run(redis_client=self.redis_client)
        mock_update.assert_called_once_with("config/scrapy_onk.json", "end_time", "2026-04-25")
        self.assertIsNone(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY))

    def test_finalize_onk_run_skips_update_when_pending_exists(self):
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")
        self.redis_client.rpush(self.module.REDIS_PENDING_KEY, "job-1")
        with patch.object(self.module, "update_json_config") as mock_update:
            self.module.finalize_onk_run(redis_client=self.redis_client)
        mock_update.assert_not_called()

    def test_finalize_onk_run_skips_update_when_processing_exists(self):
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, "job-1")
        with patch.object(self.module, "update_json_config") as mock_update:
            self.module.finalize_onk_run(redis_client=self.redis_client)
        mock_update.assert_not_called()


class TestWriteAndQueue(unittest.TestCase):
    def setUp(self):
        self.module, self.temp_dir = load_scrapy_onk()
        self.redis_client = MiniRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_write_to_disk_and_enqueue_onk_page_results(self):
        result_list = [
            {
                "title": "Movie A",
                "label": "NZB",
                "imdb_id": "tt1111111",
                "url": "https://onk.example/threads/1",
                "post_date": datetime.datetime(2026, 1, 16),
            },
            {
                "title": "Movie B",
                "label": "DIY",
                "imdb_id": "",
                "url": "https://onk.example/threads/2",
                "post_date": datetime.datetime(2026, 1, 16),
            },
        ]
        with patch.object(self.module, "normalize_release_title_for_filename", side_effect=lambda title: title), patch.object(
            self.module, "sanitize_filename", side_effect=lambda title: title
        ):
            self.module.write_to_disk(result_list)
            enqueued_count = self.module.enqueue_onk_page_results(result_list, self.redis_client)
            enqueued_again = self.module.enqueue_onk_page_results(result_list, self.redis_client)

        self.assertTrue((Path(self.module.OUTPUT_DIR) / "Movie A(NZB)[tt1111111].onk").exists())
        self.assertTrue((Path(self.module.OUTPUT_DIR) / "Movie B(DIY)[].onk").exists())
        self.assertEqual(enqueued_count, 2)
        self.assertEqual(enqueued_again, 0)
        payload = json.loads(self.redis_client.values[self.module.REDIS_PENDING_KEY][0])
        self.assertEqual(payload["url"], "https://onk.example/threads/1")
        self.assertEqual(payload["file_path"], str(Path(self.module.OUTPUT_DIR) / "Movie A(NZB)[tt1111111].onk"))
        second_payload = json.loads(self.redis_client.values[self.module.REDIS_PENDING_KEY][1])
        self.assertEqual(second_payload["url"], "https://onk.example/threads/2")

    def test_recover_onk_processing_when_pending_is_empty(self):
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, '{"url":"https://onk.example/threads/1"}')
        recovered_count = self.module.recover_onk_processing_when_pending_is_empty(self.redis_client)
        self.assertEqual(recovered_count, 1)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 1)

    def test_recover_onk_processing_skips_when_pending_already_has_tasks(self):
        self.redis_client.rpush(self.module.REDIS_PENDING_KEY, '{"url":"https://onk.example/threads/1"}')
        self.redis_client.rpush(self.module.REDIS_PROCESSING_KEY, '{"url":"https://onk.example/threads/2"}')

        recovered_count = self.module.recover_onk_processing_when_pending_is_empty(self.redis_client)

        self.assertEqual(recovered_count, 0)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PENDING_KEY), 1)
        self.assertEqual(self.redis_client.llen(self.module.REDIS_PROCESSING_KEY), 1)

    def test_enqueue_onk_page_results_keeps_queue_stage_free_of_disk_lookup(self):
        result_list = [
            {
                "title": "Movie A",
                "label": "NZB",
                "imdb_id": "",
                "url": "https://onk.example/threads/1",
                "post_date": datetime.datetime(2026, 1, 16),
            }
        ]

        with patch.object(self.module, "build_onk_file_path", return_value="predicted.onk") as mock_build:
            enqueued_count = self.module.enqueue_onk_page_results(result_list, self.redis_client)

        self.assertEqual(enqueued_count, 1)
        mock_build.assert_called_once_with(result_list[0])
        payload = json.loads(self.redis_client.values[self.module.REDIS_PENDING_KEY][0])
        self.assertEqual(payload["file_path"], "predicted.onk")

    def test_build_onk_url_index_maps_existing_onk_files(self):
        output_dir = Path(self.module.OUTPUT_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "Movie A(NZB)[tt1111111].onk").write_text(
            "https://onk.example/threads/1\n2026-01-16 00:00:00",
            encoding="utf-8",
        )
        (output_dir / "Movie B(DIY)[].onk").write_text(
            "https://onk.example/threads/2\n2026-01-16 00:00:00",
            encoding="utf-8",
        )

        result = self.module.build_onk_url_index()

        self.assertEqual(
            result,
            {
                "https://onk.example/threads/1": str(output_dir / "Movie A(NZB)[tt1111111].onk"),
                "https://onk.example/threads/2": str(output_dir / "Movie B(DIY)[].onk"),
            },
        )


class TestParseAndFetch(unittest.TestCase):
    def setUp(self):
        self.module, self.temp_dir = load_scrapy_onk()
        self.stop_time = datetime.datetime(2026, 1, 1)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_parse_forum_page_collects_new_posts_and_stops_on_old_posts(self):
        response = make_forum_response(
            build_thread_html(title="新帖子", href="/threads/new-1", label="DIY", imdb_href="https://www.imdb.com/title/tt1111111/"),
            build_thread_html(title="旧帖子", href="/threads/old-1", datetime_attr="2025-12-31T10:00:00+0000"),
        )
        with patch.object(self.module, "get_onk_response", return_value=response):
            result, stop = self.module.parse_forum_page(group_id=10, start_page=2, stop_time=self.stop_time)
        self.assertEqual(result[0]["url"], "https://onk.example/threads/new-1")
        self.assertTrue(stop)

    def test_parse_forum_page_returns_empty_when_page_has_no_threads(self):
        response = Mock(text="<html><body>empty</body></html>", content=b"<html><body>empty</body></html>")
        with patch.object(self.module, "get_onk_response", return_value=response):
            result, stop = self.module.parse_forum_page(group_id=10, start_page=1, stop_time=self.stop_time)
        self.assertEqual(result, [])
        self.assertFalse(stop)

    def test_get_onk_response_retries_on_bad_status(self):
        bad_status = Mock(status_code=503, text="A" * 10001)
        with patch.object(self.module.session, "get", return_value=bad_status):
            with self.assertRaisesRegex(Exception, "请求失败"):
                self.module.get_onk_response("https://onk.example/threads/1")

    def test_get_onk_response_allows_short_body_when_status_is_200(self):
        short_body = Mock(status_code=200, text="short")
        with patch.object(self.module.session, "get", return_value=short_body):
            result = self.module.get_onk_response("https://onk.example/threads/1")
        self.assertIs(result, short_body)


class TestQueueWrappers(unittest.TestCase):
    def setUp(self):
        self.module, self.temp_dir = load_scrapy_onk({"group_dict": {"电影": 10, "剧集": 20}})
        self.redis_client = MiniRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_enqueue_onk_posts_starts_each_group_from_first_page_and_marks_complete(self):
        with patch.object(
            self.module,
            "parse_forum_page",
            side_effect=[([{"title": "电影 A"}], False), ([{"title": "电影 B"}], True), ([], False)],
        ) as mock_parse, patch.object(
            self.module, "enqueue_onk_page_results", side_effect=[1, 1]
        ) as mock_enqueue:
            self.module.enqueue_onk_posts(datetime.datetime(2026, 1, 1), redis_client=self.redis_client)

        self.assertEqual(
            mock_parse.call_args_list,
            [
                call(10, 1, datetime.datetime(2026, 1, 1)),
                call(10, 2, datetime.datetime(2026, 1, 1)),
                call(20, 1, datetime.datetime(2026, 1, 1)),
            ],
        )
        self.assertEqual(
            mock_enqueue.call_args_list,
            [
                call([{"title": "电影 A"}], self.redis_client),
                call([{"title": "电影 B"}], self.redis_client),
            ],
        )
        self.assertEqual(self.redis_client.get(self.module.REDIS_SCAN_COMPLETE_KEY), "1")

    def test_enqueue_onk_posts_skips_when_scan_already_complete(self):
        self.redis_client.set(self.module.REDIS_SCAN_COMPLETE_KEY, "1")
        with patch.object(self.module, "parse_forum_page") as mock_parse:
            self.module.enqueue_onk_posts(datetime.datetime(2026, 1, 1), redis_client=self.redis_client)
        mock_parse.assert_not_called()

    def test_drain_onk_queue_reuses_shared_queue_flow(self):
        with patch.object(self.module, "drain_queue") as mock_drain:
            self.module.drain_onk_queue(redis_client=self.redis_client)
        self.assertEqual(mock_drain.call_args.kwargs["pending_key"], self.module.REDIS_PENDING_KEY)
        self.assertTrue(mock_drain.call_args.kwargs["keep_failed_in_processing"])


class TestVisitAndDownload(unittest.TestCase):
    def setUp(self):
        self.module, self.temp_dir = load_scrapy_onk()
        Path(self.module.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_download_drive_artifact_handles_single_and_multiple_outputs(self):
        onk_path = Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt7654321].onk"
        onk_path.write_text("https://onk.example/threads/1", encoding="utf-8")
        download_result = types.SimpleNamespace(
            drive_name="Movie.nzb",
            payload=b'<?xml version="1.0"?><nzb></nzb>',
            content_type="application/octet-stream",
            suggested_suffix=".nzb",
        )

        with patch.object(self.module, "download_gd_url", return_value=download_result):
            single_output = self.module.download_drive_artifact("https://drive.google.com/file/d/file123/view?usp=sharing", str(onk_path))
            multi_output = self.module.download_drive_artifact(
                "https://drive.google.com/file/d/file456/view?usp=sharing",
                str(onk_path),
                2,
                3,
            )

        self.assertEqual(Path(single_output).name, "Movie(NZB)[tt7654321].nzb")
        self.assertEqual(Path(multi_output).name, "Movie(NZB)[tt7654321].02.nzb")

    def test_visit_onk_url_writes_non_nzb_file_without_visiting_detail_page(self):
        result_item = {
            "title": "Movie B",
            "label": "DIY",
            "imdb_id": "",
            "url": "https://onk.example/threads/2",
            "post_date": "2026-01-16 00:00:00",
            "file_path": str(Path(self.module.OUTPUT_DIR) / "Movie B(DIY)[].onk"),
        }

        with patch.object(self.module, "get_onk_response") as mock_get:
            result = self.module.visit_onk_url(result_item)

        self.assertEqual(result, result_item["file_path"])
        self.assertTrue(Path(result_item["file_path"]).exists())
        mock_get.assert_not_called()

    def test_visit_onk_url_renames_file_and_downloads_all_drive_links(self):
        file_path = Path(self.module.OUTPUT_DIR) / "Movie(NZB)[].onk"
        response = Mock(
            text=(
                '<div class="message-cell message-cell--main">'
                'tt7654321 '
                '<a href="https://drive.google.com/file/d/file111/view?usp=sharing">Google Drive 1</a>'
                '<a href="https://drive.google.com/file/d/file222/view?usp=sharing">Google Drive 2</a>'
                '</div>'
            )
        )

        with patch.object(self.module, "get_onk_response", return_value=response), patch.object(
            self.module,
            "download_drive_artifact",
            side_effect=[
                str(Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt7654321].01.nzb"),
                str(Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt7654321].02.nzb"),
            ],
        ) as mock_download:
            result = self.module.visit_onk_url(
                {
                    "title": "Movie",
                    "label": "NZB",
                    "imdb_id": "",
                    "url": "https://onk.example/threads/1",
                    "post_date": "2026-01-16 00:00:00",
                    "file_path": str(file_path),
                }
            )

        renamed_path = Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt7654321].onk"
        self.assertFalse(file_path.exists())
        self.assertTrue(renamed_path.exists())
        self.assertEqual(
            result,
            [
                str(Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt7654321].01.nzb"),
                str(Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt7654321].02.nzb"),
            ],
        )
        self.assertEqual(
            mock_download.call_args_list,
            [
                call("https://drive.google.com/file/d/file111/view?usp=sharing", str(renamed_path), 1, 2),
                call("https://drive.google.com/file/d/file222/view?usp=sharing", str(renamed_path), 2, 2),
            ],
        )

    def test_visit_onk_url_uses_first_imdb_when_multiple_ids_exist(self):
        file_path = Path(self.module.OUTPUT_DIR) / "Movie(NZB)[].onk"
        response = Mock(
            text=(
                '<div class="message-cell message-cell--main">'
                'tt1111111 tt2222222 '
                '<a href="https://drive.google.com/file/d/file111/view?usp=sharing">Google Drive 1</a>'
                '</div>'
            )
        )

        with patch.object(self.module, "get_onk_response", return_value=response), patch.object(
            self.module, "download_drive_artifact", return_value=str(Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt1111111].nzb")
        ) as mock_download:
            result = self.module.visit_onk_url(
                {
                    "title": "Movie",
                    "label": "NZB",
                    "imdb_id": "",
                    "url": "https://onk.example/threads/1",
                    "post_date": "2026-01-16 00:00:00",
                    "file_path": str(file_path),
                }
            )

        renamed_path = Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt1111111].onk"
        self.assertEqual(result, str(Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt1111111].nzb"))
        self.assertTrue(renamed_path.exists())
        mock_download.assert_called_once_with(
            "https://drive.google.com/file/d/file111/view?usp=sharing",
            str(renamed_path),
            1,
            1,
        )

    def test_visit_onk_url_reuses_result_item_imdb_without_reparsing_post_body(self):
        file_path = Path(self.module.OUTPUT_DIR) / "Movie(NZB)[].onk"
        response = Mock(
            text=(
                '<div class="message-cell message-cell--main">'
                '<a href="https://drive.google.com/file/d/file111/view?usp=sharing">Google Drive 1</a>'
                '</div>'
            )
        )

        with patch.object(self.module, "get_onk_response", return_value=response), patch.object(
            self.module,
            "download_drive_artifact",
            return_value=str(Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt7654321].nzb"),
        ) as mock_download, patch.object(self.module, "extract_unique_imdb_id") as mock_extract:
            result = self.module.visit_onk_url(
                {
                    "title": "Movie",
                    "label": "NZB",
                    "imdb_id": "tt7654321",
                    "url": "https://onk.example/threads/1",
                    "post_date": "2026-01-16 00:00:00",
                    "file_path": str(file_path),
                }
            )

        renamed_path = Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt7654321].onk"
        self.assertEqual(result, str(Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt7654321].nzb"))
        self.assertTrue(renamed_path.exists())
        mock_download.assert_called_once_with(
            "https://drive.google.com/file/d/file111/view?usp=sharing",
            str(renamed_path),
            1,
            1,
        )
        mock_extract.assert_not_called()

    def test_visit_onk_url_skips_when_numbered_outputs_already_exist(self):
        onk_path = Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt7654321].onk"
        onk_path.write_text("https://onk.example/threads/1\n2026-01-16 00:00:00", encoding="utf-8")
        first_output = Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt7654321].01.nzb"
        second_output = Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt7654321].02.nzb"
        first_output.write_text("done1", encoding="utf-8")
        second_output.write_text("done2", encoding="utf-8")

        with patch.object(self.module, "get_onk_response") as mock_get:
            result = self.module.visit_onk_url(
                {
                    "title": "Movie",
                    "label": "NZB",
                    "imdb_id": "tt7654321",
                    "url": "https://onk.example/threads/1",
                    "post_date": "2026-01-16 00:00:00",
                    "file_path": str(onk_path),
                }
            )

        self.assertEqual(result, [str(first_output), str(second_output)])
        mock_get.assert_not_called()

    def test_visit_onk_url_recovers_renamed_path_and_skips_when_no_drive_link_exists(self):
        renamed_path = Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt7654321].onk"
        renamed_path.write_text("https://onk.example/threads/1\n2026-01-16 00:00:00", encoding="utf-8")
        no_drive_response = Mock(text='<div class="message-cell message-cell--main">tt7654321 but no drive link</div>')

        with patch.object(self.module, "get_onk_response", return_value=no_drive_response), patch.object(
            self.module, "download_drive_artifact"
        ) as mock_download, self.assertLogs(self.module.logger.name, level="ERROR") as logs:
            with self.assertRaisesRegex(RuntimeError, "帖子里没有找到 Google Drive 链接"):
                self.module.visit_onk_url(
                    {
                        "title": "Movie",
                        "label": "NZB",
                        "imdb_id": "",
                        "file_path": str(Path(self.module.OUTPUT_DIR) / "Movie(NZB)[].onk"),
                        "url": "https://onk.example/threads/1",
                        "post_date": "2026-01-16 00:00:00",
                    }
                )

        mock_download.assert_not_called()
        self.assertTrue(any("帖子里没有找到 Google Drive 链接" in line for line in logs.output))

    def test_resolve_onk_file_path_raises_when_no_matching_onk_exists(self):
        with self.assertRaisesRegex(FileNotFoundError, "未找到对应的 ONK 文件"):
            self.module.resolve_onk_file_path({"file_path": "missing.onk", "url": "https://onk.example/threads/404"})

    def test_ensure_onk_file_reuses_existing_renamed_onk_file(self):
        renamed_path = Path(self.module.OUTPUT_DIR) / "Movie(NZB)[tt7654321].onk"
        renamed_path.write_text("https://onk.example/threads/1\n2026-01-16 00:00:00", encoding="utf-8")

        result = self.module.ensure_onk_file(
            {
                "title": "Movie",
                "label": "NZB",
                "imdb_id": "",
                "url": "https://onk.example/threads/1",
                "post_date": "2026-01-16 00:00:00",
                "file_path": str(Path(self.module.OUTPUT_DIR) / "Movie(NZB)[].onk"),
            }
        )

        self.assertEqual(result, str(renamed_path))


class TestScrapyOnkMain(unittest.TestCase):
    def setUp(self):
        self.module, self.temp_dir = load_scrapy_onk()
        self.redis_client = MiniRedis()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_scrapy_onk_runs_recover_enqueue_drain_and_finalize(self):
        with patch.object(self.module, "get_redis_client", return_value=self.redis_client), patch.object(
            self.module, "get_current_end_time", return_value="2026-01-01"
        ), patch.object(self.module, "recover_onk_processing_when_pending_is_empty") as mock_recover, patch.object(
            self.module, "enqueue_onk_posts"
        ) as mock_enqueue, patch.object(self.module, "drain_onk_queue") as mock_drain, patch.object(
            self.module, "finalize_onk_run"
        ) as mock_finalize:
            self.module.scrapy_onk()

        mock_recover.assert_called_once_with(self.redis_client)
        mock_enqueue.assert_called_once_with(datetime.datetime(2026, 1, 1), redis_client=self.redis_client)
        mock_drain.assert_called_once_with(redis_client=self.redis_client)
        mock_finalize.assert_called_once_with(redis_client=self.redis_client)


if __name__ == "__main__":
    unittest.main()
