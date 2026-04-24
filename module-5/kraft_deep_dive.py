"""Module 5: KRaft Architecture — 集群元数据查询与 Controller 监测."""

import asyncio
import logging

from aiokafka.admin import AIOKafkaAdminClient

logging.getLogger("aiokafka").setLevel(logging.ERROR)

BROKERS = "localhost:19094,localhost:29094,localhost:39094"


async def describe_cluster(brokers: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    try:
        cluster = await admin.describe_cluster()
        print(f"Cluster ID:        {cluster['cluster_id']}")
        print(f"Active Controller: node_id={cluster['controller_id']}")
        print(f"\nBrokers ({len(cluster['brokers'])}):")
        for b in cluster["brokers"]:
            print(f"  node_id={b['node_id']}, host={b['host']}, port={b['port']}")
    finally:
        await admin.close()


async def list_topics(brokers: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    try:
        topics = await admin.list_topics()
        user_topics = sorted(t for t in topics if not t.startswith("_"))
        print(f"\nUser topics ({len(user_topics)}):")
        for t in user_topics:
            print(f"  {t}")

        if user_topics:
            descriptions = await admin.describe_topics(user_topics)
            print("\nPartition details:")
            for desc in descriptions:
                print(f"  {desc['topic']}: {len(desc['partitions'])} partition(s)")
    finally:
        await admin.close()


async def watch_controller(brokers: str, rounds: int = 8, interval: float = 3.0) -> None:
    """监测 Active Controller 变化，可配合 docker compose stop/start 观察重新选举."""
    print(f"\n监测 Controller（{rounds} 轮，每 {interval}s 一次）")
    print("提示：在另一个终端执行 'docker compose stop kafka-N' 触发重选举\n")
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    try:
        prev = None
        for r in range(rounds):
            try:
                cluster = await admin.describe_cluster()
                ctrl = cluster["controller_id"]
                if ctrl != prev:
                    tag = f"Controller changed: {prev} → {ctrl}" if prev is not None else f"Active Controller: node_id={ctrl}"
                    print(f"  {tag}")
                    prev = ctrl
                else:
                    print(f"  Round {r + 1}: Controller = {ctrl} (no change)")
            except Exception as e:
                print(f"  Round {r + 1}: Error - {e}")
            await asyncio.sleep(interval)
    finally:
        await admin.close()


async def main() -> None:
    print("=== 1. 集群元数据 ===")
    await describe_cluster(BROKERS)

    print("\n=== 2. Topic 列表 ===")
    await list_topics(BROKERS)

    # 取消注释以运行 Controller 监测（需配合终端操作）
    # await watch_controller(BROKERS)

    print("\n演示完成！")


if __name__ == "__main__":
    asyncio.run(main())
