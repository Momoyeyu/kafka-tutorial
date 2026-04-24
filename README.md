# Kafka Python Tutorial

> 使用 Python (aiokafka) + Docker Compose 从零学习 Apache Kafka。
> 每章节配有可交互的 Jupyter Notebook，边学边跑。

MIT License · Copyright (c) 2026 Momoyeyu

---

## 课程大纲

| 模块 | 主题 | 核心内容 |
|------|------|---------|
| [Module 1](module-1/) | **Quick Start** | 环境搭建、第一个 Producer & Consumer、offset 基础 |
| [Module 2](module-2/) | **Topics & Partitions** | 分区原理、消息路由（Key → Partition）、Admin API |
| [Module 3](module-3/) | **Consumer Groups** | 分区分配、负载均衡、Rebalance、offset 续传 |
| [Module 4](module-4/) | **Replication & Fault Tolerance** | 副本机制、ISR、acks 参数、幂等 Producer、Broker 宕机演示 |
| [Module 5](module-5/) | **KRaft Architecture** | ZooKeeper vs KRaft、Raft 选举、集群元数据查询 |
| [Module 6](module-6/) | **Advanced Producer** | 压缩、自定义 Partitioner、批量优化、事务 Producer |
| [Module 7](module-7/) | **Advanced Consumer** | 手动 offset 提交、Seek、时间戳回放、Exactly-Once 语义 |
| [Module 8](module-8/) | **Async Task Queue** | 用 Kafka 构建任务队列、Taskiq 框架集成 |

---

## 快速开始

### 1. 启动 Kafka 集群

```bash
docker compose up -d
```

3 节点 KRaft 集群启动后，各 Broker 的外部访问端口：

| Broker | 容器内地址 | 本机访问地址 |
|--------|----------|------------|
| kafka-1 | kafka-1:9092 | localhost:19094 |
| kafka-2 | kafka-2:9092 | localhost:29094 |
| kafka-3 | kafka-3:9092 | localhost:39094 |

### 2. 安装 Python 依赖

```bash
# 推荐使用 uv
uv sync

# 或使用 pip
pip install aiokafka notebook ipykernel python-snappy
```

### 3. 打开 Jupyter Notebook

```bash
jupyter notebook
```

按模块顺序学习：`module-1` → `module-2` → ... → `module-8`

### 停止集群

```bash
docker compose down        # 停止容器，保留数据
docker compose down -v     # 停止容器并清除所有数据
```

---

## 项目结构

```
kafka-tutorial/
├── docker-compose.yml          # 3 节点 KRaft Kafka 集群（仅基础设施）
├── pyproject.toml              # Python 依赖管理（uv）
├── LICENSE                     # MIT License
│
├── module-1/                   # Quick Start
│   └── quickstart.ipynb
├── module-2/                   # Topics & Partitions
│   └── topics_and_partitions.ipynb
├── module-3/                   # Consumer Groups
│   └── consumer_groups.ipynb
├── module-4/                   # Replication & Fault Tolerance
│   └── replication.ipynb
├── module-5/                   # KRaft Architecture
│   └── kraft_deep_dive.ipynb
├── module-6/                   # Advanced Producer
│   └── advanced_producer.ipynb
├── module-7/                   # Advanced Consumer
│   └── advanced_consumer.ipynb
└── module-8/                   # Async Task Queue (Taskiq)
    ├── docker-compose.yml      # Module 8 专用（扩展基础集群）
    └── taskiq_integration.ipynb
```

---

## 环境要求

| 组件 | 版本要求 |
|------|---------|
| Docker + Docker Compose | 最新稳定版 |
| Python | 3.12+ |
| uv（推荐）或 pip | 任意版本 |

### 各模块额外依赖

| 模块 | 额外依赖 | 安装命令 |
|------|---------|---------|
| Module 1–7 | 无（已包含在 `uv sync`）| — |
| Module 8 | taskiq | `uv sync --extra taskiq` |

---

## 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network                           │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   kafka-1   │  │   kafka-2   │  │   kafka-3   │         │
│  │  (node_id=1)│  │  (node_id=2)│  │  (node_id=3)│         │
│  │  broker +   │  │  broker +   │  │  broker +   │         │
│  │  controller │  │  controller │  │  controller │         │
│  │             │  │             │  │             │         │
│  │ EXTERNAL:   │  │ EXTERNAL:   │  │ EXTERNAL:   │         │
│  │ :9094       │  │ :9094       │  │ :9094       │         │
└──┼─────────────┼──┼─────────────┼──┼─────────────┼─────────┘
   │             │  │             │  │             │
   ▼             │  ▼             │  ▼             │
localhost:19094  │  localhost:29094  localhost:39094│
                 │                                 │
         ┌───────┴─────────────────────────────────┘
         │
   ┌─────┴──────────────────────────────────┐
   │   Jupyter Notebook / Python Script     │
   │   aiokafka Producer / Consumer         │
   └────────────────────────────────────────┘
```

---

## License

MIT License · Copyright (c) 2026 Momoyeyu

See [LICENSE](LICENSE) for details.
