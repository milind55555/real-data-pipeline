"""
Spark Structured Streaming consumer for the IoT pipeline.

Reads JSON events from Kafka topic 'iot_events', validates/enriches them,
then writes:
  1. Raw events → MongoDB (pipeline.events)
  2. Aggregated metrics → PostgreSQL (public.device_metrics)

Run with:
    pip install pyspark pymongo psycopg2-binary
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:spark-mongodb_2.12:10.4.0 \
                 --py-files mongodb/mongo_utils.py spark/streaming_job.py
"""
import os
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, window, avg, count, max as spark_max,
    min as spark_min, sum as spark_sum, round as spark_round, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType
)

from mongodb.mongo_utils import write_to_mongodb

# ── Configuration ─────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = "iot_events"
PG_URL = os.environ.get("PG_URL", "jdbc:postgresql://localhost:5432/pipeline")
PG_PROPS = {
    "user": os.environ.get("PG_USER", "pipeline"),
    "password": os.environ.get("PG_PASSWORD", "pipeline"),
    "driver": "org.postgresql.Driver",
}

CHECKPOINT_LOCATION = os.environ.get("CHECKPOINT_DIR", "/tmp/spark-checkpoint")

# ── Schema matching the Kafka producer ────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("device_id", StringType(), True),
    StructField("timestamp_ms", LongType(), True),
    StructField("voltage_v", DoubleType(), True),
    StructField("current_a", DoubleType(), True),
    StructField("power_w", DoubleType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("grid_frequency_hz", DoubleType(), True),
])

# ── Spark Session ─────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("IoTRealtimePipeline")
    .config("spark.sql.adaptive.enabled", "true")          # adaptive query execution
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Read stream from Kafka ────────────────────────────────────────
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# Parse JSON payload and select fields
events_df = (
    raw_df
    .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("event_time", (col("timestamp_ms") / 1000).cast("timestamp"))
    .withColumn("ingest_time", current_timestamp())
)

# ── Basic validation: drop malformed rows ─────────────────────────
valid_df = events_df.filter(
    col("device_id").isNotNull() &
    col("timestamp_ms").isNotNull() &
    col("voltage_v").between(180, 260) &
    col("current_a").between(0, 100)
)

# ── Sink 1: Raw events → MongoDB ──────────────────────────────────
mongo_query = valid_df.writeStream.outputMode("append").format("mongodb") \
    .option("spark.mongodb.write.database", "pipeline") \
    .option("spark.mongodb.write.collection", "events") \
    .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/mongo") \
    .start()

# ── Sink 2: 1-minute tumbling window aggregates → PostgreSQL ─────
agg_df = (
    valid_df
    .withWatermark("event_time", "30 seconds")
    .groupBy(
        window("event_time", "60 seconds"),
        col("device_id")
    )
    .agg(
        spark_round(avg("voltage_v"), 2).alias("avg_voltage_v"),
        spark_round(avg("current_a"), 2).alias("avg_current_a"),
        spark_round(avg("power_w"), 2).alias("avg_power_w"),
        spark_round(avg("temperature_c"), 2).alias("avg_temp_c"),
        spark_round(avg("grid_frequency_hz"), 3).alias("avg_freq_hz"),
        spark_round(spark_max("voltage_v"), 2).alias("max_voltage_v"),
        spark_round(spark_min("voltage_v"), 2).alias("min_voltage_v"),
        count("*").alias("sample_count"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "device_id",
        "avg_voltage_v", "avg_current_a", "avg_power_w",
        "avg_temp_c", "avg_freq_hz", "max_voltage_v", "min_voltage_v",
        "sample_count",
        lit("now").alias("processed_at"),
    )
)

pg_query = (
    agg_df.writeStream
    .outputMode("update")           # update aggregates as windows complete
    .format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", "device_metrics")
    .option("user", PG_PROPS["user"])
    .option("password", PG_PROPS["password"])
    .option("driver", PG_PROPS["driver"])
    .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/pg")
    .start()
)

print("Streaming job started. Watching topic:", KAFKA_TOPIC)
print("Mongo sink:", f"{CHECKPOINT_LOCATION}/mongo")
print("Postgres sink:", f"{CHECKPOINT_LOCATION}/pg")

# Await termination
spark.streams.awaitAnyTermination()