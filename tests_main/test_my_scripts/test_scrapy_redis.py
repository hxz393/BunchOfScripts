"""
针对 ``my_scripts.scrapy_redis`` 的集成测试。

这里使用真实 ``redis`` 客户端连接 ``config/scrapy_redis.json`` 指向的 Redis，
用唯一 key 前缀隔离测试数据；Redis 不可达时整组跳过。
"""

import importlib.util
import json
import sys
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

import redis

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_redis.py"
CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "scrapy_redis.json"


def load_scrapy_redis():
    """在最小依赖环境中加载 ``scrapy_redis`` 模块。"""
    helper_config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: dict(helper_config)

    spec = importlib.util.spec_from_file_location(
        f"scrapy_redis_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"my_module": fake_my_module}):
        spec.loader.exec_module(module)

    module._test_config = helper_config
    return module


class TestScrapyRedis(unittest.TestCase):
    """验证共享 Redis helper 与真实 Redis 的交互。"""

    @classmethod
    def setUpClass(cls):
        cls.module = load_scrapy_redis()
        cls.redis_client = cls.module.get_redis_client()
        try:
            cls.redis_client.ping()
        except redis.RedisError as exc:  # pragma: no cover - 仅在环境不可用时触发
            raise unittest.SkipTest(f"Redis 不可达，跳过 scrapy_redis 集成测试：{exc}")

    def setUp(self):
        prefix = f"codex:test:scrapy_redis:{uuid.uuid4().hex}"
        self.seen_key = f"{prefix}:seen"
        self.pending_key = f"{prefix}:pending"
        self.processing_key = f"{prefix}:processing"
        self.failed_key = f"{prefix}:failed"

    def tearDown(self):
        self.redis_client.delete(
            self.seen_key,
            self.pending_key,
            self.processing_key,
            self.failed_key,
        )

    def test_get_redis_client_uses_shared_config_connection(self):
        """应按共享配置创建 Redis 客户端。"""
        client = self.module.get_redis_client()
        kwargs = client.connection_pool.connection_kwargs

        self.assertEqual(kwargs["host"], self.module._test_config["redis_host"])
        self.assertEqual(kwargs["port"], self.module._test_config.get("redis_port", 6379))
        self.assertEqual(kwargs["db"], self.module._test_config.get("redis_db", 0))
        self.assertTrue(kwargs["decode_responses"])

    def test_serialize_and_deserialize_payload_round_trip_preserves_unicode(self):
        """序列化后再反序列化，应保留原始字典内容。"""
        payload = {"title": "霸王别姬", "url": "https://example.com/电影", "id": "1"}

        serialized = self.module.serialize_payload(payload)
        restored = self.module.deserialize_payload(serialized)

        self.assertEqual(restored, payload)

    def test_push_items_to_queue_deduplicates_items_and_serializes_payloads(self):
        """重复任务不应重复入队，真实入队内容应经过序列化。"""
        items = [
            {"id": "1", "url": "u1"},
            {"id": "1", "url": "u1"},
            {"id": "2", "url": "u2"},
        ]

        enqueued_count = self.module.push_items_to_queue(
            self.redis_client,
            items,
            seen_key=self.seen_key,
            pending_key=self.pending_key,
            unique_value=lambda item: item["id"],
            serializer=self.module.serialize_payload,
        )

        self.assertEqual(enqueued_count, 2)
        pending_payloads = self.redis_client.lrange(self.pending_key, 0, -1)
        self.assertEqual(
            [self.module.deserialize_payload(payload)["id"] for payload in pending_payloads],
            ["1", "2"],
        )
        self.assertEqual(self.redis_client.smembers(self.seen_key), {"1", "2"})

    def test_push_items_to_queue_returns_zero_when_items_are_empty(self):
        """空任务列表不应写入任何 Redis 数据。"""
        enqueued_count = self.module.push_items_to_queue(
            self.redis_client,
            [],
            seen_key=self.seen_key,
            pending_key=self.pending_key,
            unique_value=lambda item: item["id"],
        )

        self.assertEqual(enqueued_count, 0)
        self.assertEqual(self.redis_client.llen(self.pending_key), 0)
        self.assertEqual(self.redis_client.smembers(self.seen_key), set())

    def test_recover_processing_queue_moves_items_back_and_logs_warning(self):
        """processing 中残留的任务应恢复回 pending，并记录警告。"""
        logger = Mock()
        self.redis_client.rpush(self.pending_key, "payload-a")
        self.redis_client.rpush(self.processing_key, "payload-b")
        self.redis_client.rpush(self.processing_key, "payload-c")

        recovered_count = self.module.recover_processing_queue(
            self.redis_client,
            processing_key=self.processing_key,
            pending_key=self.pending_key,
            logger=logger,
            queue_label="TEST",
        )

        self.assertEqual(recovered_count, 2)
        self.assertEqual(self.redis_client.llen(self.processing_key), 0)
        self.assertEqual(
            self.redis_client.lrange(self.pending_key, 0, -1),
            ["payload-b", "payload-c", "payload-a"],
        )
        logger.warning.assert_called_once_with("恢复 2 条未完成的 TEST 任务回待处理队列")

    def test_recover_processing_queue_returns_zero_without_warning_when_processing_is_empty(self):
        """processing 为空时，应返回 0 且不记录 warning。"""
        logger = Mock()

        recovered_count = self.module.recover_processing_queue(
            self.redis_client,
            processing_key=self.processing_key,
            pending_key=self.pending_key,
            logger=logger,
            queue_label="TEST",
        )

        self.assertEqual(recovered_count, 0)
        logger.warning.assert_not_called()

    def test_pop_next_payload_moves_last_pending_item_to_processing(self):
        """应把 pending 末尾任务移动到 processing，并返回该 payload。"""
        self.redis_client.rpush(self.pending_key, "payload-a")
        self.redis_client.rpush(self.pending_key, "payload-b")

        payload = self.module.pop_next_payload(
            self.redis_client,
            pending_key=self.pending_key,
            processing_key=self.processing_key,
        )

        self.assertEqual(payload, "payload-b")
        self.assertEqual(self.redis_client.lrange(self.pending_key, 0, -1), ["payload-a"])
        self.assertEqual(self.redis_client.lrange(self.processing_key, 0, -1), ["payload-b"])

    def test_drain_queue_returns_zero_counts_when_pending_is_empty(self):
        """没有待处理任务时，应直接返回零统计并输出提示。"""
        logger = Mock()

        result = self.module.drain_queue(
            self.redis_client,
            pending_key=self.pending_key,
            processing_key=self.processing_key,
            failed_key=self.failed_key,
            max_workers=2,
            worker=lambda _info: None,
            logger=logger,
            queue_label="TEST",
            identify_item=lambda info: info["id"],
        )

        self.assertEqual(result, {"processed": 0, "success": 0, "failed": 0})
        logger.info.assert_called_once_with("TEST 队列为空，没有待处理任务")

    def test_drain_queue_recovers_processing_items_before_consuming(self):
        """消费前应先把 processing 残留任务恢复回 pending 并继续处理。"""
        logger = Mock()
        processed_ids = []
        pending_payload = self.module.serialize_payload({"id": "1"})
        recovered_payload = self.module.serialize_payload({"id": "2"})
        self.redis_client.rpush(self.pending_key, pending_payload)
        self.redis_client.rpush(self.processing_key, recovered_payload)

        def fake_worker(info: dict) -> None:
            processed_ids.append(info["id"])

        result = self.module.drain_queue(
            self.redis_client,
            pending_key=self.pending_key,
            processing_key=self.processing_key,
            failed_key=self.failed_key,
            max_workers=2,
            worker=fake_worker,
            logger=logger,
            queue_label="TEST",
            identify_item=lambda info: info["id"],
        )

        self.assertEqual(result, {"processed": 2, "success": 2, "failed": 0})
        self.assertEqual(set(processed_ids), {"1", "2"})
        logger.warning.assert_called_once_with("恢复 1 条未完成的 TEST 任务回待处理队列")

    def test_drain_queue_processes_success_and_failure_and_cleans_processing(self):
        """消费队列时应统计成功/失败，并把失败任务写入 failed 队列。"""
        logger = Mock()
        payload_success = self.module.serialize_payload({"id": "1"})
        payload_fail = self.module.serialize_payload({"id": "2"})
        self.redis_client.rpush(self.pending_key, payload_success)
        self.redis_client.rpush(self.pending_key, payload_fail)

        def fake_worker(info: dict) -> None:
            if info["id"] == "2":
                raise RuntimeError("boom")

        result = self.module.drain_queue(
            self.redis_client,
            pending_key=self.pending_key,
            processing_key=self.processing_key,
            failed_key=self.failed_key,
            max_workers=2,
            worker=fake_worker,
            logger=logger,
            queue_label="TEST",
            identify_item=lambda info: info["id"],
        )

        self.assertEqual(result, {"processed": 2, "success": 1, "failed": 1})
        self.assertEqual(self.redis_client.llen(self.pending_key), 0)
        self.assertEqual(self.redis_client.llen(self.processing_key), 0)
        self.assertEqual(
            [self.module.deserialize_payload(payload)["id"] for payload in self.redis_client.lrange(self.failed_key, 0, -1)],
            ["2"],
        )
        logger.error.assert_called_once()
        self.assertIn("抓取出错：2，错误：boom", logger.error.call_args[0][0])

    def test_drain_queue_uses_logger_exception_when_log_traceback_is_true(self):
        """开启 ``log_traceback`` 时，失败任务应走 ``logger.exception``。"""
        logger = Mock()
        payload_fail = self.module.serialize_payload({"id": "9"})
        self.redis_client.rpush(self.pending_key, payload_fail)

        def fake_worker(_info: dict) -> None:
            raise RuntimeError("boom")

        result = self.module.drain_queue(
            self.redis_client,
            pending_key=self.pending_key,
            processing_key=self.processing_key,
            failed_key=self.failed_key,
            max_workers=1,
            worker=fake_worker,
            logger=logger,
            queue_label="TEST",
            identify_item=lambda info: info["id"],
            log_traceback=True,
        )

        self.assertEqual(result, {"processed": 1, "success": 0, "failed": 1})
        logger.exception.assert_called_once()
        logger.error.assert_not_called()
        self.assertIn("抓取出错：9，错误：boom", logger.exception.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
