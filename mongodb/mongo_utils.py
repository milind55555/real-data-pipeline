"""
MongoDB helper utilities for the Spark streaming job.

Provides a small wrapper around the MongoDB Spark connector so the main
streaming script can call write_to_mongodb() without repeating options.
"""
import os
from pyspark.sql import DataFrame


def write_to_mongodb(
    df: DataFrame,
    database: str = "pipeline",
    collection: str = "events",
    checkpoint_dir: str = "/tmp/spark-checkpoint/mongo",
) -> None:
    """
    Write a streaming DataFrame to MongoDB using the Spark connector.

    The connector handles schema inference and upsert-like semantics when
    combined with appropriate write strategies. For streaming we use
    'append' mode so every micro-batch is appended to the collection.
    """
    (
        df.writeStream
        .outputMode("append")
        .format("mongodb")
        .option("spark.mongodb.write.database", database)
        .option("spark.mongodb.write.collection", collection)
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )


def mongo_connection_string() -> str:
    """Return a PyMongo-style connection string from env vars."""
    user = os.environ.get("MONGO_USER", "admin")
    pwd = os.environ.get("MONGO_PASSWORD", "admin")
    host = os.environ.get("MONGO_HOST", "localhost")
    port = os.environ.get("MONGO_PORT", "27017")
    return f"mongodb://{user}:{pwd}@{host}:{port}/pipeline"