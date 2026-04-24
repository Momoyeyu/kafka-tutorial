"""Module 8: Async Task Queue — aiokafka DIY 任务队列 + Taskiq 集成."""

import asyncio
import json
import logging
import time
import uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

logging.getLogger("aiokafka").setLevel(logging.ERROR)

BROKERS = "localhost:19094,localhost:29094,localhost:39094"
TASK_TOPIC = "task-queue"
RESULT_TOPIC = "task-results"

# ──────────────────────────────────────────────
# Task registry
# ──────────────────────────────────────────────

TASK_REGISTRY: dict = {}


def task(fn):
    TASK_REGISTRY[fn.__name__] = fn
    return fn


@task
async def send_email(to: str, subject: str, body: str) -> dict:
    await asyncio.sleep(0.1)
    return {"status": "sent", "to": to, "chars": len(body)}


@task
async def resize_image(url: str, width: int, height: int) -> dict:
    await asyncio.sleep(0.2)
    return {"status": "done", "output": f"{url}-{width}x{height}.jpg"}


@task
async def calculate(expression: str) -> dict:
    import math
    safe_env = {"__builtins__": {}, "math": math, "sum": sum, "range": range, "len": len}
    return {"result": eval(expression, safe_env)}


def make_task(task_name: str, **kwargs) -> bytes:
    return json.dumps({
        "task_id": str(uuid.uuid4())[:8],
        "task": task_name,
        "args": kwargs,
        "submitted_at": time.time(),
    }).encode()


# ──────────────────────────────────────────────
# Setup
# ──────────────────────────────────────────────

async def setup_topics(brokers: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    try:
        existing = await admin.list_topics()
        new_topics = [
            NewTopic(name=t, num_partitions=3, replication_factor=3)
            for t in [TASK_TOPIC, RESULT_TOPIC]
            if t not in existing
        ]
        if new_topics:
            await admin.create_topics(new_topics)
            await asyncio.sleep(1)
            print(f"✓ Created: {[t.name for t in new_topics]}")
    finally:
        await admin.close()


# ──────────────────────────────────────────────
# Producer / Worker
# ──────────────────────────────────────────────

async def submit_tasks(brokers: str, topic: str, tasks: list) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=brokers)
    await producer.start()
    try:
        for task_name, kwargs in tasks:
            msg = make_task(task_name, **kwargs)
            td = json.loads(msg)
            meta = await producer.send_and_wait(topic, msg)
            print(f"  [Submit] {td['task']}({kwargs}) → task_id={td['task_id']}, partition={meta.partition}")
    finally:
        await producer.stop()


async def run_worker(
    brokers: str, task_topic: str, result_topic: str, worker_id: str, max_tasks: int = 6
) -> int:
    consumer = AIOKafkaConsumer(
        task_topic, bootstrap_servers=brokers, group_id="worker-pool",
        auto_offset_reset="earliest", enable_auto_commit=False,
    )
    result_producer = AIOKafkaProducer(bootstrap_servers=brokers)
    await consumer.start()
    await result_producer.start()
    processed = 0
    try:
        while processed < max_tasks:
            results = await consumer.getmany(timeout_ms=2000, max_records=2)
            if not results:
                break
            for tp_msgs in results.values():
                for msg in tp_msgs:
                    td = json.loads(msg.value)
                    fn = TASK_REGISTRY.get(td["task"])
                    t0 = time.perf_counter()
                    try:
                        result = await fn(**td["args"]) if fn else {"error": f"unknown: {td['task']}"}
                        status = "success" if fn else "failed"
                    except Exception as e:
                        result, status = {"error": str(e)}, "failed"
                    elapsed = (time.perf_counter() - t0) * 1000
                    await result_producer.send_and_wait(result_topic, json.dumps({
                        "task_id": td["task_id"], "task": td["task"],
                        "status": status, "result": result,
                        "worker": worker_id, "elapsed_ms": round(elapsed, 1),
                    }).encode())
                    await consumer.commit()
                    print(f"  [{worker_id}] {td['task']}({td['task_id']}) → {status} in {elapsed:.0f}ms")
                    processed += 1
    finally:
        await consumer.stop()
        await result_producer.stop()
    return processed


async def show_results(brokers: str, result_topic: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    descriptions = await admin.describe_topics([result_topic])
    num_partitions = len(descriptions[0]["partitions"])
    await admin.close()

    consumer = AIOKafkaConsumer(bootstrap_servers=brokers, auto_offset_reset="earliest")
    tps = [TopicPartition(result_topic, p) for p in range(num_partitions)]
    consumer.assign(tps)
    await consumer.start()
    results = []
    try:
        while True:
            batch = await consumer.getmany(*tps, timeout_ms=1000, max_records=50)
            if not batch:
                break
            for tp_msgs in batch.values():
                for msg in tp_msgs:
                    results.append(json.loads(msg.value))
    finally:
        await consumer.stop()

    print(f"\n=== Task Results ({len(results)} total) ===")
    for r in results:
        print(f"  [{r['status']:7s}] {r['task']}({r['task_id']}) → {r['result']} ({r['elapsed_ms']}ms)")


# ──────────────────────────────────────────────
# Taskiq demo (optional, requires: uv sync --extra taskiq)
# ──────────────────────────────────────────────

async def taskiq_demo() -> None:
    try:
        from taskiq import InMemoryBroker
    except ImportError:
        print("\n[skip] taskiq not installed. Run: uv sync --extra taskiq")
        return

    broker = InMemoryBroker()

    @broker.task
    async def process_order(order_id: str, amount: float) -> dict:
        await asyncio.sleep(0.05)
        return {"order_id": order_id, "charged": amount, "status": "completed"}

    await broker.startup()
    print("\n=== Taskiq InMemoryBroker ===")
    t1 = await process_order.kiq("ORD-001", 99.99)
    t2 = await process_order.kiq("ORD-002", 49.50)
    for t in [t1, t2]:
        r = await t.wait_result(timeout=5)
        print(f"  Result: {r.return_value}")
    await broker.shutdown()


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

async def main() -> None:
    await setup_topics(BROKERS)

    print("\n=== 提交任务 ===")
    await submit_tasks(BROKERS, TASK_TOPIC, [
        ("send_email", {"to": "alice@example.com", "subject": "Welcome", "body": "Hello!"}),
        ("send_email", {"to": "bob@example.com", "subject": "Update", "body": "Check this"}),
        ("resize_image", {"url": "s3://bucket/photo.jpg", "width": 800, "height": 600}),
        ("resize_image", {"url": "s3://bucket/avatar.png", "width": 128, "height": 128}),
        ("calculate", {"expression": "2 ** 10"}),
        ("calculate", {"expression": "sum(range(100))"}),
    ])

    print("\n=== 启动 2 个并行 Worker ===")
    w1, w2 = await asyncio.gather(
        run_worker(BROKERS, TASK_TOPIC, RESULT_TOPIC, "worker-1", max_tasks=6),
        run_worker(BROKERS, TASK_TOPIC, RESULT_TOPIC, "worker-2", max_tasks=6),
    )
    print(f"  worker-1: {w1} tasks, worker-2: {w2} tasks")

    await show_results(BROKERS, RESULT_TOPIC)
    await taskiq_demo()

    print("\n演示完成！")


if __name__ == "__main__":
    asyncio.run(main())
