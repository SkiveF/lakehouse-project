"""Ingestion des sources vers la couche Bronze.

Principes :
  * schema explicite (declare dans config/settings.yaml), jamais inferSchema ;
  * ecriture en APPEND partitionnee par `ingestion_date`, donc bronze conserve
    l'historique et une ingestion ratee n'ecrase rien ;
  * chaque ligne porte sa provenance (`ingested_at`, `source_file`) ;
  * la derive de schema est detectee et levee, au lieu d'etre subie.

Les colonnes techniques ne sont volontairement pas prefixees par "_" : Spark
ignore les repertoires commencant par "_" lors de la decouverte de partitions.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# Colonnes ajoutees par l'ingestion, exclues de la comparaison de schema metier.
METADATA_COLUMNS = ("ingested_at", "source_file", "ingestion_date")
PARTITION_COLUMN = "ingestion_date"


def build_schema_ddl(schema_def: list[dict[str, str]]) -> str:
    """Construit une chaine DDL Spark depuis la definition YAML d'une source."""
    if not schema_def:
        raise ValueError("Schema vide : chaque source doit declarer ses colonnes")
    return ", ".join(f"`{col['name']}` {col['type']}" for col in schema_def)


def read_source_to_dataframe(spark: SparkSession, source_conf: dict[str, Any]) -> DataFrame:
    """Lit une source avec son schema declare (aucune inference)."""
    schema_ddl = build_schema_ddl(source_conf["schema"])
    reader = spark.read.schema(schema_ddl).format(source_conf.get("format", "csv"))

    for key, value in (source_conf.get("options") or {}).items():
        reader = reader.option(key, str(value))

    df = reader.load(source_conf["path"])
    logger.info("Lecture de %s avec le schema declare : %s", source_conf["path"], schema_ddl)
    return df


def with_ingestion_metadata(df: DataFrame, ingested_at: datetime, ingestion_date: date) -> DataFrame:
    """Ajoute la provenance et la colonne de partition."""
    return (
        df.withColumn("ingested_at", F.lit(ingested_at.isoformat()).cast("timestamp"))
        .withColumn("source_file", F.input_file_name())
        .withColumn(PARTITION_COLUMN, F.lit(ingestion_date.isoformat()).cast("date"))
    )


def _business_columns(schema) -> list[tuple[str, str]]:
    """(nom, type) des colonnes metier, dans l'ordre, hors colonnes techniques."""
    return [
        (field.name, field.dataType.simpleString())
        for field in schema.fields
        if field.name not in METADATA_COLUMNS
    ]


def assert_no_schema_drift(spark: SparkSession, table: str, df: DataFrame) -> None:
    """Refuse d'ecrire si le schema declare ne correspond plus a la table.

    Sans ce garde-fou, un `CREATE TABLE IF NOT EXISTS` laisse la definition Hive
    figee sur l'ancien schema pendant que les fichiers changent : les lectures
    renvoient alors des colonnes fantomes sans lever d'erreur.
    """
    if not spark.catalog.tableExists(table):
        return

    registered = _business_columns(spark.table(table).schema)
    incoming = _business_columns(df.schema)

    if registered != incoming:
        raise ValueError(
            f"Derive de schema sur {table}.\n"
            f"  table enregistree : {registered}\n"
            f"  schema declare    : {incoming}\n"
            "Mets a jour config/settings.yaml puis recree la table "
            f"(DROP TABLE {table}) avant de reingerer."
        )


def register_bronze_table(spark: SparkSession, table_name: str, path: str) -> None:
    """Enregistre la table externe et resynchronise ses partitions."""
    table = f"bronze.{table_name}"
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table}
        USING PARQUET
        LOCATION '{path}'
        """
    )
    # Nouvelle partition ajoutee sur le stockage : le metastore doit la decouvrir.
    spark.sql(f"MSCK REPAIR TABLE {table}")
    spark.sql(f"REFRESH TABLE {table}")


def write_bronze(
    df: DataFrame,
    table_name: str,
    root: str,
    ingested_at: datetime,
    ingestion_date: date,
) -> int:
    """Ecrit un lot en bronze (append, partitionne) et met a jour le metastore.

    Retourne le nombre de lignes ecrites.
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("Aucune SparkSession active")

    path = f"{root.rstrip('/')}/{table_name}/"
    enriched = with_ingestion_metadata(df, ingested_at, ingestion_date)

    assert_no_schema_drift(spark, f"bronze.{table_name}", enriched)

    row_count = enriched.count()
    (
        enriched.write.mode("append")
        .partitionBy(PARTITION_COLUMN)
        .parquet(path)
    )
    logger.info(
        "[BRONZE] %s : %s lignes ajoutees dans la partition %s=%s",
        table_name, row_count, PARTITION_COLUMN, ingestion_date.isoformat(),
    )

    register_bronze_table(spark, table_name, path)
    return row_count


# --- Compatibilite ascendante -------------------------------------------------
# Conserve pour ne pas casser un appel existant ; prefere read_source_to_dataframe.
def read_csv_to_dataframe(spark: SparkSession, file_path: str) -> DataFrame:
    """Deprecie : lecture CSV sans schema declare."""
    logger.warning(
        "read_csv_to_dataframe est deprecie (inference de schema). "
        "Utilise read_source_to_dataframe avec un schema declare."
    )
    return spark.read.csv(file_path, header=True, inferSchema=True)
