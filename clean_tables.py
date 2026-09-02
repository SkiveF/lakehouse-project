#!/usr/bin/env python3
"""Remet le lakehouse a zero. RESERVE AU DEVELOPPEMENT.

Contrairement a un simple DROP DATABASE CASCADE, ce script supprime aussi les
donnees dans MinIO : les tables des trois couches sont EXTERNES, donc les
supprimer du metastore laisserait tous les fichiers Parquet en place -- et un
"reset" qui ne reinitialise rien est un filet de securite trompeur.

Utilisation :
    spark-submit /opt/project/clean_tables.py --yes
    spark-submit /opt/project/clean_tables.py --yes --metastore-only
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from pyspark.sql import SparkSession

from spark.config import load_config, validate_bronze_config
from spark.utils import create_spark_session, delete_storage_prefix

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

LAYERS = ("gold", "silver", "bronze")
STORAGE_PREFIXES = ("s3a://bronze/", "s3a://silver/", "s3a://gold/")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reinitialise le lakehouse (destructif).")
    parser.add_argument(
        "--yes", action="store_true",
        help="Ne pas demander de confirmation (obligatoire en mode non interactif).",
    )
    parser.add_argument(
        "--metastore-only", action="store_true",
        help="Ne supprimer que les bases Hive, en conservant les fichiers dans MinIO.",
    )
    return parser.parse_args()


def confirm(skip: bool) -> None:
    if skip:
        return
    logger.warning("Ceci va SUPPRIMER toutes les donnees du lakehouse.")
    logger.warning("Interruption possible pendant 5 secondes (Ctrl+C)...")
    try:
        time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Annule.")
        sys.exit(0)


def drop_databases(spark: SparkSession) -> None:
    for database in LAYERS:
        try:
            spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
            logger.info("OK base '%s' supprimee", database)
        except Exception as error:  # noqa: BLE001
            # Journalise au lieu d'avaler : un echec ici laisse le metastore
            # dans un etat partiel qu'il faut pouvoir diagnostiquer.
            logger.error("ECHEC suppression de la base '%s' : %s", database, error)
            raise


def purge_storage(spark: SparkSession) -> None:
    for prefix in STORAGE_PREFIXES:
        delete_storage_prefix(spark, prefix)


def recreate_databases(spark: SparkSession) -> None:
    for database in reversed(LAYERS):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
        logger.info("OK base '%s' recreee (vide)", database)


def main() -> None:
    args = parse_args()
    confirm(args.yes)

    config = load_config("/opt/project/config/settings.yaml")
    validate_bronze_config(config)

    spark = create_spark_session("lakehouse_clean")
    try:
        logger.info("[1/3] Suppression des bases Hive")
        drop_databases(spark)

        if args.metastore_only:
            logger.info("[2/3] Fichiers MinIO conserves (--metastore-only)")
        else:
            logger.info("[2/3] Purge des donnees dans MinIO")
            purge_storage(spark)

        logger.info("[3/3] Recreation des bases vides")
        recreate_databases(spark)

        logger.info("Reset termine.")
        logger.info("Etapes suivantes : init_tables.py, main.py --layer bronze, dbt run")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
