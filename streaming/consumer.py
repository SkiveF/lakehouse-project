"""Spark Structured Streaming consumer for Kafka orders topic.

Reads orders from Kafka, validates them, and writes to MinIO Bronze layer.
"""

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# spark-submit n'ajoute que le dossier du script au sys.path : on remonte a la
# racine du projet pour reutiliser la fabrique de SparkSession commune.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from spark.utils import create_spark_session  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Define the schema for incoming Kafka messages
ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)


def consume_orders(
    spark: SparkSession,
    kafka_brokers: str,
    topic: str,
    bronze_path: str,
    checkpoint_path: str,
) -> None:
    """Read orders from Kafka, validate, and write to Bronze."""

    # Read from Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    logger.info(f"Connected to Kafka: {kafka_brokers}, topic: {topic}")

    # Parse JSON and extract fields
    parsed_df = (
        kafka_df
        .select(
            from_json(col("value").cast("string"), ORDER_SCHEMA).alias("order")
        )
        .select(
            col("order.order_id"),
            col("order.customer_id"),
            col("order.amount"),
            col("order.currency"),
            col("order.status"),
            col("order.created_at"),
            current_timestamp().alias("processed_at"),
        )
    )

    # Filter invalid orders (optional; in production you'd log these separately)
    valid_df = parsed_df.filter(
        (col("order_id").isNotNull()) &
        (col("customer_id") > 0) &
        (col("amount") > 0) &
        (col("currency").isNotNull()) &
        (col("status").isNotNull())
    )

    valid_df.printSchema()

    # Write to Bronze (MinIO S3A) in Parquet format with checkpoint
    query = (
        valid_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", bronze_path)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    logger.info(f"Streaming query started. Writing to {bronze_path}")
    logger.info(f"Checkpoint: {checkpoint_path}")

    # Wait for termination (run indefinitely)
    query.awaitTermination()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming consumer for Kafka orders.")
    parser.add_argument(
        "--kafka-brokers",
        default="kafka:9092",
        help="Kafka bootstrap servers (docker: kafka:9092, local: localhost:9094).",
    )
    parser.add_argument("--topic", default="orders", help="Kafka topic to subscribe to.")
    parser.add_argument(
        "--bronze-path",
        default="s3a://bronze/orders_stream/",
        help="Chemin S3A de destination. Distinct de s3a://bronze/orders/, ecrit en overwrite par l'ingestion batch.",
    )
    parser.add_argument(
        "--checkpoint-path",
        default="s3a://bronze/checkpoints/orders_stream/",
        help="Checkpoint path for streaming state.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session(
        "kafka_orders_consumer",
        extra_conf={"spark.sql.streaming.schemaInference": "true"},
    )

    try:
        consume_orders(
            spark,
            args.kafka_brokers,
            args.topic,
            args.bronze_path,
            args.checkpoint_path,
        )
    except Exception as e:
        logger.error(f"Streaming error: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()

