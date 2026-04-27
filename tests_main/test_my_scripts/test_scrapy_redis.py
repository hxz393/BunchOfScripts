"""
针对 ``my_scripts.scrapy_redis`` 的单元测试。

这里不依赖真实 Redis 服务，也不会读取本地真实 ``my_module`` 实现。
目标是只验证共享 Redis helper 的核心行为：
1. 配置注入与客户端构造参数。
2. payload 序列化、去重入队、processing 恢复。
3. 队列消费时的成功、失败、致命异常分支。
"""

import copy
import importlib.util
import sys
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_redis.py"


class DummyRedis:
    """最小 ``redis.Redis`` 替身，只记录构造参数。"""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.connection_pool = types.SimpleNamespace(connection_kwargs=kwargs)


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
    """用于测试队列行为的内存版 Redis。"""

    def __init__(self):
        self.sets = {}
        self.lists = {}
        self.eval_calls = []

    def pipeline(self):
        return FakeRedisPipeline(self)

    def sadd(self, key: str, value: str) -> int:
        members = self.sets.setdefault(key, set())
        if value in members:
            return 0
        members.add(value)
        return 1

    def smembers(self, key: str) -> set[str]:
        return self.sets.get(key, set()).copy()

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
        kept = []
        for item in items:
            if item == value and removed < count:
                removed += 1
                continue
            kept.append(item)
        self.lists[key] = kept
        return removed

    def llen(self, key: str) -> int:
        return len(self.lists.get(key, []))

    def lrange(self, key: str, start: int, end: int) -> list[str]:
        items = self.lists.get(key, [])
        if end == -1:
            end = len(items) - 1
        return items[start:end + 1]

    def eval(self, script: str, numkeys: int, *keys_and_args):
        self.eval_calls.append((script, numkeys, keys_and_args))
        keys = keys_and_args[:numkeys]
        args = keys_and_args[numkeys:]
        if len(keys) != 2:
            raise AssertionError("expected seen_key and pending_key")
        if len(args) % 2 != 0:
            raise AssertionError("expected alternating unique_value/payload args")

        seen_key, pending_key = keys
        enqueued = 0
        for index in range(0, len(args), 2):
            unique_value = args[index]
            payload = args[index + 1]
            if self.sadd(seen_key, unique_value):
                self.rpush(pending_key, payload)
                enqueued += 1
        return enqueued


def load_scrapy_redis(config: dict | None = None):
    """在隔离依赖环境中加载 ``scrapy_redis`` 模块。"""
    helper_config = {
        "redis_host": "127.0.0.1",
        "redis_port": 6379,
        "redis_db": 0,
    }
    if config:
        helper_config.update(config)

    fake_my_module = types.ModuleType("my_module")
    fake_my_module.read_json_to_dict = lambda _path: copy.deepcopy(helper_config)

    fake_redis = types.ModuleType("redis")
    fake_redis.Redis = DummyRedis

    spec = importlib.util.spec_from_file_location(
        f"scrapy_redis_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"my_module": fake_my_module, "redis": fake_redis}):
        spec.loader.exec_module(module)

    module._test_config = helper_config
    return module


class TestModuleLoad(unittest.TestCase):
    """验证模块导入与客户端构造。"""

    def setUp(self):
        self.module = load_scrapy_redis()

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


class TestQueueHelpers(unittest.TestCase):
    """验证入队、恢复和出队辅助逻辑。"""

    def setUp(self):
        self.module = load_scrapy_redis()
        self.redis_client = FakeRedis()
        self.seen_key = "test:seen"
        self.pending_key = "test:pending"
        self.processing_key = "test:processing"
        self.failed_key = "test:failed"

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
        self.assertEqual(len(self.redis_client.eval_calls), 1)

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


class TestDrainQueue(unittest.TestCase):
    """验证消费队列时的成功、失败与停止策略。"""

    def setUp(self):
        self.module = load_scrapy_redis()
        self.redis_client = FakeRedis()
        self.pending_key = "test:pending"
        self.processing_key = "test:processing"
        self.failed_key = "test:failed"

    def test_drain_queue_raises_when_failed_key_is_missing(self):
        """默认失败分流模式下，必须提供 ``failed_key``。"""
        with self.assertRaisesRegex(ValueError, "failed_key 不能为空"):
            self.module.drain_queue(
                self.redis_client,
                pending_key=self.pending_key,
                processing_key=self.processing_key,
                max_workers=1,
                worker=lambda _info: None,
                logger=Mock(),
                queue_label="TEST",
                identify_item=lambda info: info["id"],
            )

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

    def test_drain_queue_can_skip_processing_recovery_on_start(self):
        """显式关闭启动恢复时，应只消费 pending，不处理 processing 残留。"""
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
            recover_processing_on_start=False,
        )

        self.assertEqual(result, {"processed": 1, "success": 1, "failed": 0})
        self.assertEqual(processed_ids, ["1"])
        self.assertEqual(
            self.redis_client.lrange(self.processing_key, 0, -1),
            [recovered_payload],
        )
        logger.warning.assert_not_called()

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

    def test_drain_queue_can_keep_failed_items_in_processing(self):
        """显式要求时，普通失败任务应保留在 processing 中。"""
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
            max_workers=2,
            worker=fake_worker,
            logger=logger,
            queue_label="TEST",
            identify_item=lambda info: info["id"],
            keep_failed_in_processing=True,
        )

        self.assertEqual(result, {"processed": 2, "success": 1, "failed": 1})
        self.assertEqual(self.redis_client.llen(self.pending_key), 0)
        self.assertEqual(
            [self.module.deserialize_payload(payload)["id"] for payload in self.redis_client.lrange(self.processing_key, 0, -1)],
            ["2"],
        )
        self.assertEqual(self.redis_client.llen(self.failed_key), 0)
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

    def test_drain_queue_requeues_fatal_item_and_raises(self):
        """致命异常应停止继续处理，并把当前任务放回 pending。"""
        logger = Mock()
        payload_fail = self.module.serialize_payload({"id": "fatal"})
        self.redis_client.rpush(self.pending_key, payload_fail)

        def fake_worker(_info: dict) -> None:
            raise RuntimeError("fatal boom")

        with self.assertRaisesRegex(RuntimeError, "fatal boom"):
            self.module.drain_queue(
                self.redis_client,
                pending_key=self.pending_key,
                processing_key=self.processing_key,
                failed_key=self.failed_key,
                max_workers=1,
                worker=fake_worker,
                logger=logger,
                queue_label="TEST",
                identify_item=lambda info: info["id"],
                abort_on_exception=lambda exc: isinstance(exc, RuntimeError),
            )

        self.assertEqual(self.redis_client.lrange(self.pending_key, 0, -1), [payload_fail])
        self.assertEqual(self.redis_client.llen(self.processing_key), 0)
        self.assertEqual(self.redis_client.llen(self.failed_key), 0)
        logger.error.assert_called_once()
        self.assertIn("TEST 检测到致命错误，停止继续处理：fatal，错误：fatal boom", logger.error.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
