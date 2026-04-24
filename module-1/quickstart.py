"""Module 1: Quick Start — 第一个 Producer & Consumer."""

import asyncio
import logging
import time

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

logging.getLogger("aiokafka").setLevel(logging.ERROR)

BROKERS = "localhost:19094,localhost:29094,localhost:39094"
TOPIC = "quickstart-topic"
STREAM_TOPIC = "quickstart-stream"
GROUP = "quickstart-group"


async def wait_for_kafka(brokers: str, retries: int = 20, delay: float = 3.0) -> bool:
    print("正在检测 Kafka 集群...")
    for i in range(retries):
        try:
            producer = AIOKafkaProducer(bootstrap_servers=brokers)
            await producer.start()
            await producer.stop()
            consumer = AIOKafkaConsumer(
                bootstrap_servers=brokers,
                group_id="__kafka_healthcheck__",
                enable_auto_commit=False,
            )
            await consumer.start()
            await consumer.stop()
            print("✓ Kafka 集群已就绪！")
            return True
        except Exception:
            print(f"  等待中... ({i + 1}/{retries})")
            await asyncio.sleep(delay)
    print("✗ 连接超时，请确认 docker compose up -d 已执行")
    return False


async def create_topic(
    brokers: str,
    topic: str,
    num_partitions: int = 1,
    replication_factor: int = 3,
) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    try:
        existing = await admin.list_topics()
        if topic in existing:
            print(f"Topic '{topic}' 已存在，跳过创建")
            return
        await admin.create_topics(
            [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)]
        )
        print(f"✓ Topic '{topic}' 创建成功（分区={num_partitions}, 副本={replication_factor}）")
    finally:
        await admin.close()


async def produce(brokers: str, topic: str, count: int = 5) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=brokers)
    await producer.start()
    try:
        for i in range(1, count + 1):
            message = f"Hello Kafka! #{i}"
            meta = await producer.send_and_wait(topic, message.encode())
            print(f"[Producer] 发送: {message!r} -> partition={meta.partition}, offset={meta.offset}")
    finally:
        await producer.stop()
    print("[Producer] 完成")


async def consume(brokers: str, topic: str, group: str, max_messages: int = 5) -> None:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=brokers,
        group_id=group,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        count = 0
        async for msg in consumer:
            print(f"[Consumer] 收到: {msg.value.decode()!r} <- partition={msg.partition}, offset={msg.offset}")
            count += 1
            if count >= max_messages:
                break
    finally:
        await consumer.stop()
    print("[Consumer] 完成")


async def stream_producer(brokers: str, topic: str, ready: asyncio.Event, count: int = 5) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=brokers)
    await producer.start()
    try:
        await ready.wait()
        for i in range(1, count + 1):
            msg = f"event-{i} @ {time.strftime('%H:%M:%S')}"
            await producer.send_and_wait(topic, msg.encode())
            print(f"  [P] → {msg}")
            await asyncio.sleep(0.5)
    finally:
        await producer.stop()


async def stream_consumer(
    brokers: str, topic: str, group: str, ready: asyncio.Event, count: int = 5
) -> None:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=brokers,
        group_id=group,
        auto_offset_reset="latest",
    )
    await consumer.start()
    try:
        await consumer.getmany(timeout_ms=500)
        ready.set()
        received = 0
        async for msg in consumer:
            print(f"  [C] ← {msg.value.decode()} (offset={msg.offset})")
            received += 1
            if received >= count:
                break
    finally:
        await consumer.stop()


async def consume_all(brokers: str, topic: str, group: str) -> None:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=brokers,
        group_id=group,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    count = 0
    try:
        while True:
            results = await consumer.getmany(timeout_ms=1500, max_records=50)
            if not results:
                break
            for tp_msgs in results.values():
                for msg in tp_msgs:
                    print(f"  offset={msg.offset:3d} | {msg.value.decode()}")
                    count += 1
    finally:
        await consumer.stop()
    print(f"共读取 {count} 条消息")


async def main() -> None:
    if not await wait_for_kafka(BROKERS):
        return

    print("\n=== 2. 创建 Topic ===")
    await create_topic(BROKERS, TOPIC)

    print("\n=== 3. Producer 发送消息 ===")
    await produce(BROKERS, TOPIC)

    print("\n=== 4. Consumer 接收消息 ===")
    await consume(BROKERS, TOPIC, GROUP)

    print("\n=== 5. 实时流演示 ===")
    await create_topic(BROKERS, STREAM_TOPIC)
    ready = asyncio.Event()
    await asyncio.gather(
        stream_producer(BROKERS, STREAM_TOPIC, ready, count=5),
        stream_consumer(BROKERS, STREAM_TOPIC, "stream-group", ready, count=5),
    )

    print("\n=== 6. 观察全部 Offset ===")
    await consume_all(BROKERS, TOPIC, "inspect-group-1")

    print("\n演示完成！")


if __name__ == "__main__":
    asyncio.run(main())
