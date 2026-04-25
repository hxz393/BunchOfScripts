"""
抓取脚本共用的 Redis 队列辅助函数。

:author: assassing
:contact: https://github.com/hxz393
:copyright: Copyright 2026, hxz393. 保留所有权利。
"""
import json
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Callable

import redis
from my_module import read_json_to_dict

CONFIG_PATH = 'config/scrapy_redis.json'
CONFIG = read_json_to_dict(CONFIG_PATH)  # Redis 配置文件

REDIS_HOST = CONFIG['redis_host']  # Redis 主机
REDIS_PORT = CONFIG.get('redis_port', 6379)  # Redis 端口
REDIS_DB = CONFIG.get('redis_db', 0)  # Redis DB


def get_redis_client() -> redis.Redis:
    """按统一配置创建 Redis 客户端。"""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True,
    )


def serialize_payload(payload: dict) -> str:
    """将任务字典序列化为 Redis 中保存的字符串。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def deserialize_payload(payload: str) -> dict:
    """将 Redis 中保存的字符串还原为任务字典。"""
    return json.loads(payload)


def push_items_to_queue(
        redis_client: redis.Redis,
        items: list[dict],
        *,
        seen_key: str,
        pending_key: str,
        unique_value: Callable[[dict], str],
        serializer: Callable[[dict], str] = serialize_payload,
) -> int:
    """
    将新任务写入 Redis 队列，并使用 set 做去重。
    返回本次真正入队的任务数量。
    """
    if not items:
        return 0

    pipe = redis_client.pipeline()
    for item in items:
        pipe.sadd(seen_key, unique_value(item))
    added_results = pipe.execute()

    queue_pipe = redis_client.pipeline()
    enqueued_count = 0
    for added, item in zip(added_results, items):
        if added:
            queue_pipe.rpush(pending_key, serializer(item))
            enqueued_count += 1

    if enqueued_count:
        queue_pipe.execute()

    return enqueued_count


def recover_processing_queue(
        redis_client: redis.Redis,
        *,
        processing_key: str,
        pending_key: str,
        logger,
        queue_label: str,
) -> int:
    """将中断时残留在 processing 队列中的任务恢复回 pending 队列。"""
    recovered_count = 0
    while True:
        payload = redis_client.rpoplpush(processing_key, pending_key)
        if not payload:
            break
        recovered_count += 1

    if recovered_count:
        logger.warning(f"恢复 {recovered_count} 条未完成的 {queue_label} 任务回待处理队列")

    return recovered_count


def pop_next_payload(redis_client: redis.Redis, *, pending_key: str, processing_key: str) -> str | None:
    """从 pending 队列中取出一个任务，并移动到 processing 队列。"""
    return redis_client.rpoplpush(pending_key, processing_key)


def drain_queue(
        redis_client: redis.Redis,
        *,
        pending_key: str,
        processing_key: str,
        failed_key: str,
        max_workers: int,
        worker: Callable[[dict], None],
        deserialize: Callable[[str], dict] = deserialize_payload,
        logger,
        queue_label: str,
        identify_item: Callable[[dict], str],
        progress_every: int | None = None,
        log_traceback: bool = False,
) -> dict[str, int]:
    """从 Redis 队列中取任务，使用线程池持续消费。"""
    recover_processing_queue(
        redis_client,
        processing_key=processing_key,
        pending_key=pending_key,
        logger=logger,
        queue_label=queue_label,
    )

    initial_pending_count = redis_client.llen(pending_key)
    if initial_pending_count == 0:
        logger.info(f"{queue_label} 队列为空，没有待处理任务")
        return {"processed": 0, "success": 0, "failed": 0}

    logger.info(f"{queue_label} 队列开始处理：待处理 {initial_pending_count} 条")
    processed_count = 0
    success_count = 0
    failed_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {}

        while True:
            while len(future_to_task) < max_workers:
                payload = pop_next_payload(
                    redis_client,
                    pending_key=pending_key,
                    processing_key=processing_key,
                )
                if not payload:
                    break
                info = deserialize(payload)
                future = executor.submit(worker, info)
                future_to_task[future] = (payload, info)

            if not future_to_task:
                break

            done, _ = wait(tuple(future_to_task), return_when=FIRST_COMPLETED)
            for future in done:
                payload, info = future_to_task.pop(future)
                processed_count += 1
                try:
                    future.result()
                    success_count += 1
                except Exception as exc:
                    failed_count += 1
                    message = f"抓取出错：{identify_item(info)}，错误：{exc}"
                    if log_traceback:
                        logger.exception(message)
                    else:
                        logger.error(message)
                    redis_client.rpush(failed_key, payload)
                finally:
                    redis_client.lrem(processing_key, 1, payload)

                if progress_every and processed_count % progress_every == 0:
                    remaining_count = redis_client.llen(pending_key) + len(future_to_task)
                    logger.info(
                        f"{queue_label} 队列进度：已处理 {processed_count} 条，成功 {success_count} 条，"
                        f"失败 {failed_count} 条，剩余约 {remaining_count} 条"
                    )

    logger.info(
        f"{queue_label} 队列处理完成：总计 {processed_count} 条，成功 {success_count} 条，失败 {failed_count} 条"
    )
    return {"processed": processed_count, "success": success_count, "failed": failed_count}
