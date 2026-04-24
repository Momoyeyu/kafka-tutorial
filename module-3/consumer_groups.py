"""Module 3: Consumer Groups — 负载均衡与 offset 续传."""

import asyncio
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

logging.getLogger("aiokafka").setLevel(logging.ERROR)

BROKERS = "localhost:19094,localhost:29094,localhost:39094"
TOPIC = "consumer-groups-demo"
NUM_PARTITIONS = 3


async def setup(brokers: str, topic: str, num_partitions: int) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    try:
        existing = await admin.list_topics()
        if topic in existing:
            await admin.delete_topics([topic])
            await asyncio.sleep(2)
        await admin.create_topics(
            [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=3)]
        )
        await asyncio.sleep(1)
    finally:
        await admin.close()

    producer = AIOKafkaProducer(bootstrap_servers=brokers)
    await producer.start()
    try:
        for i in range(12):
            key = f"key-{i % num_partitions}"
            await producer.send_and_wait(topic, key=key.encode(), value=f"event-{i:02d}".encode())
    finally:
        await producer.stop()
    print(f"✓ 准备完成：{num_partitions} 个分区，12 条消息")


async def consumer_worker(
    brokers: str,
    topic: str,
    group: str,
    consumer_id: str,
    result_queue: asyncio.Queue,
) -> None:
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=brokers, group_id=group, auto_offset_reset="earliest"
    )
    await consumer.start()
    msgs: list = []
    try:
        await asyncio.sleep(1.5)  # 等待 rebalance 完成
        assigned = {tp.partition for tp in consumer.assignment()}
        while True:
            results = await consumer.getmany(timeout_ms=1000, max_records=20)
            if not results:
                break
            for tp_msgs in results.values():
                msgs.extend(tp_msgs)
    finally:
        await consumer.stop()
    await result_queue.put((consumer_id, sorted(assigned), len(msgs)))


async def demo_consumer_group(brokers: str, topic: str, num_consumers: int) -> None:
    group = f"demo-group-{num_consumers}c"
    queue: asyncio.Queue = asyncio.Queue()
    await asyncio.gather(
        *[consumer_worker(brokers, topic, group, f"C{i}", queue) for i in range(num_consumers)]
    )
    results = []
    while not queue.empty():
        results.append(await queue.get())
    print(f"\n=== {num_consumers} 个 Consumer ===")
    for cid, partitions, count in sorted(results):
        print(f"  {cid}: partitions={partitions}, messages={count}")


async def main() -> None:
    await setup(BROKERS, TOPIC, NUM_PARTITIONS)

    await demo_consumer_group(BROKERS, TOPIC, num_consumers=3)
    await demo_consumer_group(BROKERS, TOPIC, num_consumers=1)
    await demo_consumer_group(BROKERS, TOPIC, num_consumers=4)

    print("\n=== 不同 Group 独立消费 ===")

    async def consume_as_group(group_id: str, label: str) -> None:
        consumer = AIOKafkaConsumer(
            TOPIC, bootstrap_servers=BROKERS, group_id=group_id, auto_offset_reset="earliest"
        )
        await consumer.start()
        count = 0
        try:
            while True:
                results = await consumer.getmany(timeout_ms=1500, max_records=50)
                if not results:
                    break
                for tp_msgs in results.values():
                    count += len(tp_msgs)
        finally:
            await consumer.stop()
        print(f"  {label} ({group_id!r}): consumed {count} messages")

    await asyncio.gather(
        consume_as_group("analytics-group", "Analytics"),
        consume_as_group("audit-group", "Audit    "),
    )

    print("\n=== Offset 续传演示 ===")
    persistent_group = "persistent-group"

    consumer = AIOKafkaConsumer(
        TOPIC, bootstrap_servers=BROKERS, group_id=persistent_group, auto_offset_reset="earliest"
    )
    await consumer.start()
    count = 0
    try:
        async for msg in consumer:
            print(f"  [Run 1] partition={msg.partition}, offset={msg.offset}: {msg.value.decode()}")
            count += 1
            if count >= 6:
                break
    finally:
        await consumer.stop()

    consumer = AIOKafkaConsumer(
        TOPIC, bootstrap_servers=BROKERS, group_id=persistent_group, auto_offset_reset="earliest"
    )
    await consumer.start()
    remainder = 0
    try:
        while True:
            results = await consumer.getmany(timeout_ms=2000, max_records=50)
            if not results:
                break
            for tp_msgs in results.values():
                for msg in tp_msgs:
                    print(f"  [Run 2] partition={msg.partition}, offset={msg.offset}: {msg.value.decode()}")
                    remainder += 1
    finally:
        await consumer.stop()
    print(f"  [Run 2] 续传读取 {remainder} 条（无重复）")

    print("\n演示完成！")


if __name__ == "__main__":
    asyncio.run(main())
