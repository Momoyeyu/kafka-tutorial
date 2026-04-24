"""Module 2: Topics & Partitions — 分区原理与消息路由."""

import asyncio
import logging
from collections import defaultdict

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.partitioner import DefaultPartitioner

logging.getLogger("aiokafka").setLevel(logging.ERROR)

BROKERS = "localhost:19094,localhost:29094,localhost:39094"
TOPIC = "partitions-demo"
NUM_PARTITIONS = 3


async def recreate_topic(
    brokers: str, topic: str, num_partitions: int, replication_factor: int = 3
) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    try:
        existing = await admin.list_topics()
        if topic in existing:
            await admin.delete_topics([topic])
            await asyncio.sleep(1)
        await admin.create_topics(
            [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)]
        )
        await asyncio.sleep(1)
        descriptions = await admin.describe_topics([topic])
        for desc in descriptions:
            for p in sorted(desc["partitions"], key=lambda x: x["partition"]):
                print(f"  P{p['partition']}: leader={p['leader']}, isr={p['isr']}")
    finally:
        await admin.close()
    print(f"✓ Topic '{topic}' 就绪（{num_partitions} 个分区）")


async def produce_no_key(brokers: str, topic: str, count: int = 9) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=brokers)
    await producer.start()
    partition_counts: dict[int, int] = defaultdict(int)
    try:
        for i in range(count):
            meta = await producer.send_and_wait(topic, f"msg-{i:02d}".encode())
            partition_counts[meta.partition] += 1
            print(f"  msg-{i:02d} → partition={meta.partition}, offset={meta.offset}")
    finally:
        await producer.stop()
    print(f"分区分布: {dict(sorted(partition_counts.items()))}")


async def produce_with_key(brokers: str, topic: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=brokers)
    await producer.start()
    try:
        # murmur2 hash: alice→P0, dave→P1, eve→P2 (3 partitions)
        users = [("alice", "P0"), ("dave", "P1"), ("eve", "P2")]
        for user, expected in users:
            for i in range(3):
                meta = await producer.send_and_wait(
                    topic, key=user.encode(), value=f"{user}: event-{i}".encode()
                )
                print(f"  key={user!r:8} (→{expected}) | partition={meta.partition}, offset={meta.offset}")
    finally:
        await producer.stop()


async def inspect_partitions(brokers: str, topic: str, num_partitions: int) -> None:
    consumer = AIOKafkaConsumer(bootstrap_servers=brokers, auto_offset_reset="earliest")
    tps = [TopicPartition(topic, p) for p in range(num_partitions)]
    consumer.assign(tps)
    await consumer.start()
    partition_msgs: dict[int, list] = defaultdict(list)
    try:
        while True:
            results = await consumer.getmany(*tps, timeout_ms=1000, max_records=100)
            if not results:
                break
            for tp, msgs in results.items():
                for msg in msgs:
                    partition_msgs[msg.partition].append((msg.offset, msg.key, msg.value.decode()))
    finally:
        await consumer.stop()

    for p in sorted(partition_msgs):
        print(f"\nPartition {p} ({len(partition_msgs[p])} 条消息):")
        for offset, key, value in partition_msgs[p]:
            key_str = key.decode() if key else None
            print(f"  offset={offset:3d} | key={key_str!r:8} | {value}")


async def describe_topic(brokers: str, topic: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    try:
        descriptions = await admin.describe_topics([topic])
        for desc in descriptions:
            print(f"Topic: {desc['topic']}")
            for p in sorted(desc["partitions"], key=lambda x: x["partition"]):
                print(f"  Partition {p['partition']}: leader={p['leader']}, replicas={p['replicas']}, isr={p['isr']}")
    finally:
        await admin.close()


async def main() -> None:
    print("=== 0. Key → Partition 映射预览 ===")
    partitioner = DefaultPartitioner()
    parts = list(range(NUM_PARTITIONS))
    for k in ["alice", "bob", "charlie", "dave", "eve"]:
        p = partitioner(k.encode(), parts, parts)
        print(f"  {k!r:12} → partition {p}")

    print("\n=== 1. 创建多分区 Topic ===")
    await recreate_topic(BROKERS, TOPIC, NUM_PARTITIONS)

    print("\n=== 2. 无 Key 发送（round-robin）===")
    await produce_no_key(BROKERS, TOPIC)

    print("\n=== 3. 带 Key 发送 ===")
    await produce_with_key(BROKERS, TOPIC)

    print("\n=== 4. 按分区消费 ===")
    await inspect_partitions(BROKERS, TOPIC, NUM_PARTITIONS)

    print("\n=== 5. Topic 元数据 ===")
    await describe_topic(BROKERS, TOPIC)

    print("\n演示完成！")


if __name__ == "__main__":
    asyncio.run(main())
