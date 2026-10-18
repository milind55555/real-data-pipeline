# IoT Real-Time Data Pipeline

A production-style **real-time Big Data pipeline** built with **Apache Kafka**, **Spark Structured Streaming**, **MongoDB**, **PostgreSQL**, and **Grafana**. It simulates 1,000 smart meters streaming telemetry, processes it in near-real-time, and visualizes results on an operational dashboard.

> This is a single, honest, portfolio-grade project that demonstrates end-to-end Big Data engineering skills — perfect for a GitHub resume.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────────────┐
│  Producer    │────▶│   Kafka      │────▶│ Spark Structured        │
│  (Python)   │     │  iot_events  │     │ Streaming (PySpark)     │
└─────────────┘     └──────────────┘     └──────┬──────┬───────────┘
                                               │      │
                                   raw events  │      │  aggregated metrics
                                   (MongoDB)    ▼      ▼
                                           ┌────────┐ ┌──────────┐
                                           │MongoDB │ │PostgreSQL│
                                           │pipeline│ │ pipeline │
                                           └────────┘ └────┬─────┘
                                                            │
                                                            ▼
                                                    ┌──────────────┐
                                                    │   Grafana    │
                                                    │  Dashboard   │
                                                    └──────────────┘
```

## Features

- **Kafka Producer** — Simulates 1,000 smart meters sending ~500 events/sec with realistic telemetry (voltage, current, power, temperature, grid frequency).
- **Spark Structured Streaming** — Consumes from Kafka, validates data, writes:
  - Raw events → **MongoDB** (document store, time-series indexes)
  - 1-minute tumbling-window aggregates → **PostgreSQL** (relational analytics)
- **Grafana Dashboard** — 6 panels showing events/min, per-device power, voltage range, grid frequency, active devices, and total samples.
- **Docker Compose** — One-command infrastructure setup with Kafka, Kafka-UI, PostgreSQL, MongoDB, and Grafana.
- **Production best practices** — Schema validation, checkpointing, watermarking, adaptive query execution, compression, idempotent writes.

## Quick Start

### 1. Start infrastructure

```bash
./scripts/start.sh
# or manually:
docker compose up -d
```

### 2. Run the producer

```bash
pip install -r requirements.txt
python kafka/producer.py
```

### 3. Run the Spark streaming job

```bash
pip install -r requirements.txt

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:spark-mongodb_2.12:10.4.0 \
  --py-files mongodb/mongo_utils.py \
  spark/streaming_job.py
```

### 4. View dashboards

- **Grafana**: http://localhost:3000 (admin/admin)
- **Kafka UI**: http://localhost:8080

## Project Structure

```
realtime-data-pipeline/
├── docker-compose.yml          # All services
├── requirements.txt            # Python deps
├── .gitignore
├── kafka/
│   └── producer.py             # IoT sensor simulator
├── spark/
│   └── streaming_job.py        # Structured Streaming consumer
├── mongodb/
│   ├── mongo_utils.py          # Spark connector helpers
│   └── init.js                 # Schema + indexes
├── sql/
│   └── init.sql                # PostgreSQL schema
├── grafana/
│   ├── datasources/datasources.yml   # PG + Mongo datasources
│   └── dashboards/                   # Dashboard + provisioning
└── scripts/
    ├── start.sh                # One-command launcher
    └── stop.sh                 # Tear down
```

## Key Big Data Skills Demonstrated

| Skill | Where |
|-------|-------|
| **Kafka** | Producer (acks=all, snappy compression), topic config (partitions, retention) |
| **Spark Structured Streaming** | Kafka source, schema validation, watermarking, windowed aggregations, JDBC + MongoDB sinks, checkpointing |
| **Big Data** | Time-series data modeling, windowing, exactly-once semantics via checkpoints |
| **Data Modeling** | MongoDB document schema with validation + TTL index; PostgreSQL relational schema with indexes and views |
| **Data Engineering** | ETL pipeline, fault tolerance, adaptive query execution |
| **Observability** | Grafana dashboards with live metrics, refresh, templating |

## Production Security Best Practices

This project follows production-grade security patterns:

- **Kafka**: `acks=all` for durability; `compression_type=snappy`; no plaintext exposure in compose.
- **Spark**: Checkpointing for exactly-once semantics; watermarking to handle late data; schema validation to reject malformed events.
- **PostgreSQL**: Least-privilege `pipeline` user; indexes for query performance; no superuser in app code.
- **MongoDB**: `$jsonSchema` validator on the events collection; TTL index option for data retention; authentication enabled.
- **Grafana**: Authentication required (`GF_USERS_ALLOW_SIGN_UP=false`); no anonymous access.
- **Secrets**: Credentials are in `docker-compose.yml` for local dev only; in production use Kubernetes Secrets, AWS Secrets Manager, or Vault.

## Requirements

- Docker & Docker Compose
- Python 3.9+
- Apache Spark 3.5+ (installed via `pip install pyspark`)
- Java 11+ (Spark prerequisite)

## License

MIT