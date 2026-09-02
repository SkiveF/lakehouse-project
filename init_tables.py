#!/usr/bin/env python3
"""Initialise la structure du lakehouse : bases Hive et tables externes Bronze.

Idempotent : peut etre relance sans effet de bord. Sur un lakehouse vierge, les
tables Bronze ne peuvent pas encore etre creees (aucun fichier a lire) : c'est
attendu, l'ingestion les creera.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from spark.config import load_config, validate_bronze_config
from spark.utils import create_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

LAYERS = ("bronze", "silver", "gold")


def init_databases(spark: SparkSession) -> None:
    """Cree une base Hive par couche."""
    for database in LAYERS:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
        logger.info("OK base '%s' prete", database)


def init_bronze_tables(spark: SparkSession, sources: dict, root: str) -> None:
    """Declare les tables externes Bronze et resynchronise leurs partitions."""
    for table_name in sources:
        table = f"bronze.{table_name}"
        path = f"{root.rstrip('/')}/{table_name}/"
        try:
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {table}
                USING PARQUET
                LOCATION '{path}'
                """
            )
            # Bronze est partitionne par ingestion_date : sans MSCK REPAIR le
            # metastore ignore les partitions ajoutees depuis sa creation.
            spark.sql(f"MSCK REPAIR TABLE {table}")
            spark.sql(f"REFRESH TABLE {table}")
            partitions = spark.sql(f"SHOW PARTITIONS {table}").count()
            logger.info("OK %s valide (%s partition(s))", table, partitions)
        except Exception as error:  # noqa: BLE001 - lakehouse vierge = cas normal
            logger.warning(
                "%s pas encore materialisee (sera creee par l'ingestion) : %s",
                table, error,
            )


def main() -> None:
    logger.info("Initialisation du lakehouse...")

    config = load_config("/opt/project/config/settings.yaml")
    sources = validate_bronze_config(config)
    root = config["bronze"].get("root", "s3a://bronze")

    spark = create_spark_session("lakehouse_init")
    try:
        logger.info("[1/2] Bases Hive")
        init_databases(spark)

        logger.info("[2/2] Tables externes Bronze")
        init_bronze_tables(spark, sources, root)

        logger.info("Initialisation terminee.")
        logger.info("Etapes suivantes : spark-submit main.py --layer bronze, puis dbt run")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
