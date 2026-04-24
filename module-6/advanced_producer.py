"""Module 6: Advanced Producer — 压缩、自定义 Partitioner、批量优化与事务."""

import asyncio
import json
import logging
import time

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.partitioner import DefaultPartitioner

logging.getLogger("aiokafka").setLevel(logging.ERROR)

BROKERS = "localhost:19094,localhost:29094,localhost:39094"
TOPIC = "advanced-producer-demo"
TOPIC_A = "tx-output-a"
TOPIC_B = "tx-output-b"


async def ensure_topics(brokers: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    try:
        existing = await admin.list_topics()
        new_topics = [
            NewTopic(name=t, num_partitions=n, replication_factor=3)
            for t, n in [(TOPIC, 4), (TOPIC_A, 1), (TOPIC_B, 1)]
            if t not in existing
        ]
        if new_topics:
            await admin.create_topics(new_topics)
            await asyncio.sleep(1)
            print(f"✓ Created: {[t.name for t in new_topics]}")
    finally:
        await admin.close()


async def compression_benchmark(brokers: str, topic: str, count: int = 100) -> None:
    print("\n=== 压缩基准测试 ===")
    for codec in [None, "gzip", "snappy", "lz4", "zstd"]:
        producer = AIOKafkaProducer(bootstrap_servers=brokers, compression_type=codec, acks="all")
        await producer.start()
        start = time.perf_counter()
        try:
            for i in range(count):
                payload = json.dumps({"i": i, "url": f"/products/item-{i}", "codec": str(codec)}).encode()
                await producer.send_and_wait(topic, payload)
        finally:
            await producer.stop()
        elapsed = (time.perf_counter() - start) * 1000
        print(f"  compression={codec or 'none':8s} | {count} msgs | {elapsed:.0f} ms")


class RegionPartitioner(DefaultPartitioner):
    """将特定地区 key 路由到固定分区，其余回退到默认哈希."""

    _MAP = {b"cn": 0, b"us": 1, b"eu": 2}

    def __call__(self, key, all_partitions, available):
        if key in self._MAP and self._MAP[key] in all_partitions:
            return self._MAP[key]
        return super().__call__(key, all_partitions, available)


async def custom_partitioner_demo(brokers: str, topic: str) -> None:
    print("\n=== 自定义 Partitioner ===")
    producer = AIOKafkaProducer(bootstrap_servers=brokers, partitioner=RegionPartitioner())
    await producer.start()
    try:
        for region, expected in [("cn", 0), ("us", 1), ("eu", 2), ("jp", "?")]:
            meta = await producer.send_and_wait(topic, key=region.encode(), value=region.encode())
            print(f"  region={region!r} → partition={meta.partition} (expected={expected})")
    finally:
        await producer.stop()


async def batch_benchmark(brokers: str, topic: str) -> None:
    print("\n=== 批量发送调优 ===")
    count = 500
    for linger, batch in [(0, 16 * 1024), (5, 64 * 1024), (20, 256 * 1024)]:
        producer = AIOKafkaProducer(
            bootstrap_servers=brokers, linger_ms=linger, max_batch_size=batch, acks=1
        )
        await producer.start()
        start = time.perf_counter()
        try:
            tasks = [producer.send(topic, f"batch-{i}".encode()) for i in range(count)]
            await asyncio.gather(*tasks)
            await producer.flush()
        finally:
            await producer.stop()
        elapsed = (time.perf_counter() - start) * 1000
        print(f"  linger={linger:3d}ms batch={batch//1024:4d}KB | {elapsed:.0f}ms | {count/elapsed*1000:.0f} msg/s")


async def transaction_demo(brokers: str) -> None:
    print("\n=== 事务 Producer ===")
    producer = AIOKafkaProducer(
        bootstrap_servers=brokers,
        transactional_id="demo-tx-producer",
        enable_idempotence=True,
        acks="all",
    )
    await producer.start()
    try:
        await producer.begin_transaction()
        await producer.send_and_wait(TOPIC_A, b"order-confirmed")
        await producer.send_and_wait(TOPIC_B, b"inventory-deducted")
        await producer.commit_transaction()
        print("  ✓ Committed: both messages visible")

        await producer.begin_transaction()
        await producer.send_and_wait(TOPIC_A, b"will-be-aborted")
        await producer.abort_transaction()
        print("  ✗ Aborted: message not visible")
    finally:
        await producer.stop()


async def main() -> None:
    await ensure_topics(BROKERS)
    await compression_benchmark(BROKERS, TOPIC)
    await custom_partitioner_demo(BROKERS, TOPIC)
    await batch_benchmark(BROKERS, TOPIC)
    await transaction_demo(BROKERS)
    print("\n演示完成！")


if __name__ == "__main__":
    asyncio.run(main())
