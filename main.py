"""Kafka 基础教程：演示 Producer 发送消息，Consumer 接收消息。"""

import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
TOPIC = "demo-topic"
GROUP = "demo-group"


async def wait_for_kafka():
    """等待 Kafka 集群就绪。"""
    print("等待 Kafka 集群就绪...")
    for _ in range(30):
        try:
            producer = AIOKafkaProducer(bootstrap_servers=BROKERS)
            await producer.start()
            await producer.stop()
            print("Kafka 已就绪！\n")
            return
        except Exception:
            await asyncio.sleep(3)
    raise RuntimeError("Kafka 启动超时")


async def produce():
    """发送 5 条消息到 demo-topic。"""
    producer = AIOKafkaProducer(bootstrap_servers=BROKERS)
    await producer.start()
    try:
        for i in range(1, 6):
            msg = f"消息 #{i}".encode()
            meta = await producer.send_and_wait(TOPIC, msg)
            print(f"[Producer] 发送: {msg.decode()}  -> partition={meta.partition}, offset={meta.offset}")
    finally:
        await producer.stop()


async def consume():
    """消费 demo-topic 中的消息，读完 5 条后退出。"""
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKERS,
        group_id=GROUP,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        count = 0
        async for msg in consumer:
            print(f"[Consumer] 收到: {msg.value.decode()}  <- partition={msg.partition}, offset={msg.offset}")
            count += 1
            if count >= 5:
                break
    finally:
        await consumer.stop()


async def main():
    await wait_for_kafka()
    await produce()
    print()
    await consume()
    print("\n演示完成！")


asyncio.run(main())
