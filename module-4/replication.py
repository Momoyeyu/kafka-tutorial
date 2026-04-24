"""Module 4: Replication & Fault Tolerance — 副本机制与容错.

Broker 宕机演示需要配合终端手动操作：
    docker compose stop kafka-1   # 触发 Leader 重选举
    docker compose start kafka-1  # 恢复 Broker
"""

import asyncio
import logging
import time

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

logging.getLogger("aiokafka").setLevel(logging.ERROR)

BROKERS = "localhost:19094,localhost:29094,localhost:39094"
TOPIC = "reliable-topic"


async def create_reliable_topic(brokers: str, topic: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    try:
        existing = await admin.list_topics()
        if topic not in existing:
            await admin.create_topics(
                [NewTopic(
                    name=topic,
                    num_partitions=3,
                    replication_factor=3,
                    topic_configs={"min.insync.replicas": "2"},
                )]
            )
            print(f"✓ Topic '{topic}' 创建成功（RF=3, min.insync.replicas=2）")
        else:
            print(f"Topic '{topic}' 已存在")

        descriptions = await admin.describe_topics([topic])
        for desc in descriptions:
            for p in sorted(desc["partitions"], key=lambda x: x["partition"]):
                print(f"  P{p['partition']}: leader={p['leader']}, replicas={p['replicas']}, isr={p['isr']}")
    finally:
        await admin.close()


async def benchmark_acks(brokers: str, topic: str) -> None:
    print("\n=== acks 基准测试 ===")
    for acks, label in [(0, "fire-and-forget"), (1, "leader-ack"), ("all", "all-isr-ack")]:
        producer = AIOKafkaProducer(bootstrap_servers=brokers, acks=acks)
        await producer.start()
        start = time.perf_counter()
        try:
            for i in range(20):
                await producer.send_and_wait(topic, f"acks={acks} msg-{i}".encode())
        finally:
            await producer.stop()
        elapsed = (time.perf_counter() - start) * 1000
        print(f"  {label:20s} acks={str(acks):<4} | 20 msgs | {elapsed:.0f} ms | {elapsed/20:.1f} ms/msg")


async def idempotent_demo(brokers: str, topic: str) -> None:
    print("\n=== 幂等 Producer ===")
    producer = AIOKafkaProducer(bootstrap_servers=brokers, enable_idempotence=True)
    await producer.start()
    try:
        for i in range(5):
            meta = await producer.send_and_wait(topic, f"idempotent-{i}".encode())
            print(f"  idempotent-{i} → partition={meta.partition}, offset={meta.offset}")
    finally:
        await producer.stop()
    print("幂等 Producer 保证：即使重试，每条消息仅被写入一次")


async def monitor_topic(brokers: str, topic: str, rounds: int = 10, interval: float = 3.0) -> None:
    """持续监测 Topic 副本状态，可配合 docker compose stop/start 观察变化."""
    print(f"\n=== 监测副本状态（{rounds} 轮，每 {interval}s 一次）===")
    print("提示：在另一个终端执行 'docker compose stop kafka-1' 观察 Leader 重选举\n")
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    try:
        for r in range(rounds):
            print(f"--- Round {r + 1} ---")
            try:
                descriptions = await admin.describe_topics([topic])
                for desc in descriptions:
                    for p in sorted(desc["partitions"], key=lambda x: x["partition"]):
                        alert = " ⚠ ISR degraded!" if len(p["isr"]) < 3 else ""
                        print(
                            f"  P{p['partition']}: leader={p['leader']:2d} | "
                            f"isr={sorted(p['isr'])}{alert}"
                        )
            except Exception as e:
                print(f"  Error: {e}")
            await asyncio.sleep(interval)
    finally:
        await admin.close()


async def main() -> None:
    print("=== 1. 创建高可靠 Topic ===")
    await create_reliable_topic(BROKERS, TOPIC)

    await benchmark_acks(BROKERS, TOPIC)
    await idempotent_demo(BROKERS, TOPIC)

    # 取消注释以运行 Broker 宕机演示（需配合终端操作）
    # await monitor_topic(BROKERS, TOPIC)

    print("\n演示完成！")


if __name__ == "__main__":
    asyncio.run(main())
