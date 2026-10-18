#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────
# start.sh — convenience launcher for the IoT real-time pipeline.
# Usage: ./scripts/start.sh
# ────────────────────────────────────────────────────────────────────
set -euo pipefail

echo "==> Starting infrastructure (Kafka, PostgreSQL, MongoDB, Grafana)..."
docker compose up -d

echo "==> Waiting for Kafka to accept connections..."
for i in $(seq 1 30); do
  if docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "==> Creating Kafka topic 'iot_events'..."
docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic iot_events \
  --partitions 6 --replication-factor 1 --retention-ms 604800000

echo ""
echo "Infrastructure ready. Next steps:"
echo "  1. Producer:   python kafka/producer.py"
echo "  2. Spark job:  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:spark-mongodb_2.12:10.4.0 --py-files mongodb/mongo_utils.py spark/streaming_job.py"
echo "  3. Grafana:    http://localhost:3000  (admin/admin)"
echo "  4. Kafka UI:   http://localhost:8080"