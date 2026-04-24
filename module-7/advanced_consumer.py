"""Module 7: Advanced Consumer — 手动提交、Seek 与精确一次语义."""

import asyncio
import logging
import time

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

logging.getLogger("aiokafka").setLevel(logging.ERROR)

BROKERS = "localhost:19094,localhost:29094,localhost:39094"
TOPIC = "advanced-consumer-demo"
OUTPUT_TOPIC = "eos-output"


async def setup(brokers: str, count: int = 20) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    try:
        existing = await admin.list_topics()
        for t in [TOPIC, OUTPUT_TOPIC]:
            if t in existing:
                await admin.delete_topics([t])
                await asyncio.sleep(2)
            await admin.create_topics([NewTopic(name=t, num_partitions=1, replication_factor=3)])
            await asyncio.sleep(1)
    finally:
        await admin.close()

    producer = AIOKafkaProducer(bootstrap_servers=brokers)
    await producer.start()
    try:
        for i in range(count):
            await producer.send_and_wait(TOPIC, f"record-{i:03d}".encode())
    finally:
        await producer.stop()
    print(f"✓ Seeded {count} records")


async def manual_commit_demo(brokers: str, topic: str) -> None:
    print("\n=== 手动 offset 提交（At-Least-Once）===")
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=brokers, group_id="manual-commit-group",
        auto_offset_reset="earliest", enable_auto_commit=False,
    )
    await consumer.start()
    total = 0
    try:
        while True:
            results = await consumer.getmany(timeout_ms=1500, max_records=5)
            if not results:
                break
            batch = [msg for msgs in results.values() for msg in msgs]
            values = [m.value.decode() for m in batch]
            print(f"  Processing batch: {values}")
            await consumer.commit()
            print(f"  ✓ Committed up to offset {batch[-1].offset}")
            total += len(batch)
    finally:
        await consumer.stop()
    print(f"  Total: {total} messages")


async def seek_demo(brokers: str, topic: str, seek_to: int = 10) -> None:
    print(f"\n=== Seek to offset {seek_to} ===")
    consumer = AIOKafkaConsumer(bootstrap_servers=brokers, auto_offset_reset="earliest")
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    await consumer.start()
    try:
        consumer.seek(tp, seek_to)
        results = await consumer.getmany(tp, timeout_ms=2000, max_records=5)
        for msg in results.get(tp, []):
            print(f"  offset={msg.offset}: {msg.value.decode()}")
    finally:
        await consumer.stop()


async def offset_info_demo(brokers: str, topic: str) -> None:
    print("\n=== 分区 offset 信息 ===")
    consumer = AIOKafkaConsumer(bootstrap_servers=brokers)
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    await consumer.start()
    try:
        end = await consumer.end_offsets([tp])
        begin = await consumer.beginning_offsets([tp])
        print(f"  beginning={begin[tp]}, end={end[tp]}, total={end[tp] - begin[tp]}")
    finally:
        await consumer.stop()


async def timestamp_seek_demo(brokers: str, topic: str, seconds_ago: int = 300) -> None:
    print(f"\n=== Seek by timestamp (last {seconds_ago}s) ===")
    target_ts = int((time.time() - seconds_ago) * 1000)
    consumer = AIOKafkaConsumer(bootstrap_servers=brokers)
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    await consumer.start()
    try:
        offsets = await consumer.offsets_for_times({tp: target_ts})
        ot = offsets[tp]
        if ot is None:
            print(f"  No messages after {seconds_ago}s ago")
            return
        print(f"  Seeking to offset={ot.offset}")
        consumer.seek(tp, ot.offset)
        msgs = []
        while True:
            results = await consumer.getmany(tp, timeout_ms=1000, max_records=100)
            if not results or not results.get(tp):
                break
            msgs.extend(results[tp])
        print(f"  Found {len(msgs)} messages in the last {seconds_ago}s")
        for msg in msgs[:3]:
            print(f"  offset={msg.offset}: {msg.value.decode()}")
    finally:
        await consumer.stop()


async def exactly_once_demo(brokers: str, input_topic: str, output_topic: str, count: int = 5) -> None:
    print("\n=== 精确一次语义（EOS）===")
    group = "eos-group"
    consumer = AIOKafkaConsumer(
        input_topic, bootstrap_servers=brokers, group_id=group,
        auto_offset_reset="earliest", enable_auto_commit=False,
        isolation_level="read_committed",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=brokers,
        transactional_id=f"{group}-tx",
        enable_idempotence=True,
        acks="all",
    )
    await consumer.start()
    await producer.start()
    processed = 0
    try:
        async for msg in consumer:
            result = msg.value.decode().upper()
            await producer.begin_transaction()
            try:
                await producer.send(output_topic, result.encode())
                await producer.send_offsets_to_transaction(
                    {TopicPartition(input_topic, msg.partition): msg.offset + 1}, group
                )
                await producer.commit_transaction()
                print(f"  EOS: {msg.value.decode()!r} → {result!r} (offset={msg.offset})")
            except Exception as e:
                await producer.abort_transaction()
                raise
            processed += 1
            if processed >= count:
                break
    finally:
        await consumer.stop()
        await producer.stop()
    print(f"  Processed {processed} messages with exactly-once semantics")


async def main() -> None:
    await setup(BROKERS)
    await manual_commit_demo(BROKERS, TOPIC)
    await seek_demo(BROKERS, TOPIC, seek_to=10)
    await offset_info_demo(BROKERS, TOPIC)
    await timestamp_seek_demo(BROKERS, TOPIC)
    await exactly_once_demo(BROKERS, TOPIC, OUTPUT_TOPIC)
    print("\n演示完成！")


if __name__ == "__main__":
    asyncio.run(main())
