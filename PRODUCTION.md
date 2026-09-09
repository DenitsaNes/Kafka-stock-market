# Production Architecture Evolution

This document explains the current deployment, the target production architecture, and why the project intentionally stays on a single-node setup today.

---

## Current Architecture

The current deployment is a **single-node development stack** on AWS EC2:

```text
┌─────────────────────────────────────────────────────────────┐
│                         AWS EC2 (t2.micro)                  │
│  ┌─────────────┐   ┌─────────────┐   ┌──────────────────┐ │
│  │ Zookeeper   │   │ Kafka 3.8.0 │   │ Python services  │ │
│  │  (1 node)   │◄──►│  (1 broker) │◄──►│ producers        │ │
│  └─────────────┘   └─────────────┘   │ consumers        │ │
│                                      │ Streamlit        │ │
│                                      └──────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ AWS S3 data lake  │
                    │ raw/ + parquet/   │
                    └───────────────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ Amazon Athena     │
                    └───────────────────┘
```

### Why this is the right choice for a portfolio project

- **Cost**: A single `t2.micro` is enough to demonstrate every concept end to end.
- **Simplicity**: One terminal, one `start_all.sh`, no networking complexity.
- **Learning value**: It clearly shows that I understand how Kafka brokers, Zookeeper, producers, consumers, and topics interact.

### Current limitations

- **Single broker** = no fault tolerance. If the EC2 instance fails, Kafka is down.
- **Single Zookeeper** = same issue.
- **No schema registry** = producers and consumers share the schema via `schema.py`.
- **No monitoring** = no Grafana/CloudWatch metrics on consumer lag or broker health.
- **Manual deploy** = `git pull` and restart scripts on EC2.

---

## Target Production Architecture

For a real production workload, the stack would evolve like this:

```text
                            Finnhub WebSocket
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              AWS VPC                                      │
│  ┌───────────────────────────────────────────────────────────────────┐   │
│  │                      Amazon MSK                                   │   │
│  │  ┌────────────┐   ┌────────────┐   ┌────────────┐                │   │
│  │  │ Broker 1   │◄─►│ Broker 2   │◄─►│ Broker 3   │   (multi-AZ)   │   │
│  │  │ us-east-1a │   │ us-east-1b │   │ us-east-1c │                │   │
│  │  └────────────┘   └────────────┘   └────────────┘                │   │
│  └───────────────────────────────────────────────────────────────────┘   │
│                                    ▲                                    │
│         ┌────────────────────────┼────────────────────────┐             │
│         │                        │                        │             │
│         ▼                        ▼                        ▼             │
│  ┌──────────────┐        ┌──────────────┐        ┌──────────────┐      │
│  │ S3 Sink      │        │ Dashboard    │        │ Other        │      │
│  │ Consumer     │        │ Consumer     │        │ Consumers    │      │
│  │ (consumer    │        │ (consumer    │        │ (consumer    │      │
│  │  group 1)    │        │  group 2)    │        │  group N)    │      │
│  └──────┬───────┘        └──────────────┘        └──────────────┘      │
│         │                                                               │
│         ▼                                                               │
│  ┌───────────────────┐                                                  │
│  │ AWS S3 data lake  │                                                  │
│  │ raw/ + parquet/   │                                                  │
│  └───────────────────┘                                                  │
│         │                                                               │
│         ▼                                                               │
│  ┌───────────────────┐       ┌───────────────────┐                      │
│  │ AWS Glue Crawler  │──────►│ Amazon Athena     │                      │
│  │ (or Schema Registry│       │                   │                      │
│  └───────────────────┘       └───────────────────┘                      │
│                                                                          │
│  ┌───────────────────┐       ┌───────────────────┐                      │
│  │ CloudWatch        │       │ Grafana           │                      │
│  │ (broker metrics)  │       │ (consumer lag,    │                      │
│  └───────────────────┘       │  dashboards)      │                      │
│                              └───────────────────┘                      │
└─────────────────────────────────────────────────────────────────────────┘
```

### Key production upgrades

| Component | Current | Production |
|-----------|---------|------------|
| Kafka cluster | 1 broker on EC2 | Amazon MSK with 3 brokers across 3 AZs |
| Zookeeper | 1 node on EC2 | Managed by MSK (or KRaft mode) |
| Consumers | Single instances | Multiple instances per consumer group, auto-scaled |
| Schema | `schema.py` shared file | Confluent Schema Registry or AWS Glue Schema Registry |
| S3 sink | Custom Python consumer | Kafka Connect S3 Sink Connector |
| Monitoring | Log files | CloudWatch + Grafana dashboards |
| Deployment | Manual scripts | CI/CD pipeline (GitHub Actions → EC2/ECS) |
| Fault tolerance | None | Replicated partitions, multiple brokers |

### Why these changes matter

- **Amazon MSK** removes the operational burden of running Zookeeper and broker upgrades.
- **Multiple consumer groups** let S3 sink, dashboard, and future consumers read independently without competing for partitions.
- **Kafka Connect S3 Sink** is more reliable than a custom Python consumer for high-throughput workloads.
- **Schema Registry** enforces the contract between producers and consumers.
- **Monitoring** alerts you when consumer lag grows or a broker goes offline.

---

## Intentionally Out of Scope for This Project

These are valuable but would distract from the core data-engineering story:

- Apache Spark
- Apache Airflow
- Kubernetes / Terraform
- Machine learning / trading algorithms
- Multi-microservices architecture

The goal of this repo is to show an end-to-end streaming pipeline with clean code, schema validation, tests, and CI — not to operate a full production trading platform.

---

## How to Evolve This Repo

If you want to move toward production, do it in this order:

1. Add monitoring: CloudWatch agent on EC2, Grafana dashboard for consumer lag.
2. Replace the custom S3 sink with Kafka Connect S3 Sink.
3. Add a Schema Registry (Confluent or AWS Glue).
4. Migrate from single-node Kafka to Amazon MSK.
5. Add CI/CD deployment from GitHub Actions.

Each step keeps the pipeline working and adds real production value.
