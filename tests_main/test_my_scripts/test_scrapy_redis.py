"""
针对 ``my_scripts.scrapy_redis`` 的测试。

默认使用 ``fakeredis[lua]`` 运行单元测试，直接覆盖真实 ``EVAL`` 路径，
不依赖外部 Redis 服务。

如需额外运行真实 Redis 冒烟测试，设置环境变量
``SCRAPY_REDIS_RUN_REAL=1``。
"""

import copy
import importlib.util
import json
import os
import sys
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

import redis

try:
    import fakeredis
except ImportError:  # pragma: no cover - 由依赖安装状态决定
    fakeredis = None

MODULE_PATH = Path(__file__).resolve().parents[2] / "my_scripts" / "scrapy_redis.py"
CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "scrapy_redis.json"
RUN_REAL_REDIS_ENV = "SCRAPY_REDIS_RUN_REAL"


class DummyRedis:
    """最小 ``redis.Redis`` 替身，只记录构造参数。"""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.connection_pool = types.SimpleNamespace(connection_kwargs=kwargs)


def load_scrapy_redis(config: dict | None = None, *, redis_module=None):
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

    if redis_module is None:
        redis_module = types.ModuleType("redis")
        redis_module.Redis = DummyRedis

    spec = importlib.util.spec_from_file_location(
        f"scrapy_redis_test_{uuid.uuid4().hex}",
        MODULE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"my_module": fake_my_module, "redis": redis_module}):
        spec.loader.exec_module(module)

    module._test_config = helper_config
    return module


def load_real_redis_config() -> dict:
    """读取真实 Redis 集成测试所需的配置。"""
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


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


@unittest.skipIf(fakeredis is None, "fakeredis[lua] is not installed")
class FakeredisTestCase(unittest.TestCase):
    """为默认单元测试提供真实 Lua 的 fakeredis 客户端。"""

    @classmethod
    def setUpClass(cls):
        cls.module = load_scrapy_redis()

    def setUp(self):
        self.redis_client = fakeredis.FakeRedis(decode_responses=True)
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


class TestQueueHelpers(FakeredisTestCase):
    """验证入队、恢复和出队辅助逻辑。"""

    def test_push_items_to_queue_deduplicates_items_and_serializes_payloads(self):
        """重复任务不应重复入队，真实入队内容应经过 Lua 序列化入队。"""
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


class TestDrainQueue(FakeredisTestCase):
    """验证消费队列时的成功、失败与停止策略。"""

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


@unittest.skipUnless(
    os.environ.get(RUN_REAL_REDIS_ENV) == "1",
    f"set {RUN_REAL_REDIS_ENV}=1 to run real Redis integration tests",
)
class TestRealRedisIntegration(unittest.TestCase):
    """显式开启时才运行的真实 Redis 冒烟测试。"""

    @classmethod
    def setUpClass(cls):
        cls.real_config = load_real_redis_config()
        cls.module = load_scrapy_redis(config=cls.real_config, redis_module=redis)
        cls.redis_client = cls.module.get_redis_client()
        try:
            cls.redis_client.ping()
        except redis.RedisError as exc:  # pragma: no cover - 仅在真实环境不可用时触发
            raise unittest.SkipTest(f"Redis 不可达，跳过真实集成测试：{exc}")

    def setUp(self):
        prefix = f"codex:itest:scrapy_redis:{uuid.uuid4().hex}"
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

    def test_push_items_to_queue_executes_lua_on_real_redis(self):
        """真实 Redis 下应通过 Lua 原子完成去重与入队。"""
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
        self.assertEqual(self.redis_client.smembers(self.seen_key), {"1", "2"})
        self.assertEqual(
            [self.module.deserialize_payload(payload)["id"] for payload in self.redis_client.lrange(self.pending_key, 0, -1)],
            ["1", "2"],
        )

    def test_drain_queue_smoke_runs_against_real_redis(self):
        """真实 Redis 下的基本消费流程应保持可用。"""
        logger = Mock()
        processed_ids = []
        self.redis_client.rpush(self.pending_key, self.module.serialize_payload({"id": "1"}))
        self.redis_client.rpush(self.pending_key, self.module.serialize_payload({"id": "2"}))

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
        self.assertEqual(self.redis_client.llen(self.pending_key), 0)
        self.assertEqual(self.redis_client.llen(self.processing_key), 0)
        self.assertEqual(self.redis_client.llen(self.failed_key), 0)


if __name__ == "__main__":
    unittest.main()
