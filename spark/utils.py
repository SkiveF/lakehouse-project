"""Fabrique unique de SparkSession pour tout le projet.

Tous les points d'entree (main.py, init_tables.py, validate_tables.py,
clean_tables.py, streaming/consumer.py) passent par ici : une seule definition
de la configuration S3A, un seul endroit a modifier.
"""

from __future__ import annotations

import logging
import os
from typing import Mapping

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

DEFAULT_S3A_ENDPOINT = "http://minio:9000"


def s3a_configuration() -> dict[str, str]:
    """Configuration S3A/MinIO depuis l'environnement, avec echec explicite."""
    access_key = os.getenv("SPARK_S3A_ACCESS_KEY")
    secret_key = os.getenv("SPARK_S3A_SECRET_KEY")

    if not access_key or not secret_key:
        raise EnvironmentError(
            "Credentials S3A manquants. Renseigne SPARK_S3A_ACCESS_KEY et "
            "SPARK_S3A_SECRET_KEY dans docker/.env"
        )

    endpoint = os.getenv("MINIO_ENDPOINT", DEFAULT_S3A_ENDPOINT)

    return {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.access.key": access_key,
        "spark.hadoop.fs.s3a.secret.key": secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    }


def create_spark_session(
    app_name: str,
    extra_conf: Mapping[str, str] | None = None,
) -> SparkSession:
    """Cree (ou recupere) une SparkSession configuree pour MinIO et Hive.

    Args:
        app_name: nom de l'application Spark.
        extra_conf: options supplementaires, fusionnees apres la config S3A.
    """
    builder = SparkSession.builder.appName(app_name)

    for key, value in s3a_configuration().items():
        builder = builder.config(key, value)

    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    session = builder.enableHiveSupport().getOrCreate()
    logger.info(
        "SparkSession '%s' prete (endpoint MinIO : %s)",
        app_name, os.getenv("MINIO_ENDPOINT", DEFAULT_S3A_ENDPOINT),
    )
    return session


def delete_storage_prefix(spark: SparkSession, uri: str) -> int:
    """Supprime recursivement un prefixe de stockage (s3a://...).

    Passe par l'API Hadoop FileSystem exposee par la JVM : pas de dependance
    supplementaire (boto3, mc) et cela reutilise la configuration S3A de la
    session.

    La racine d'un bucket est traitee a part : S3A ne peut pas supprimer le
    bucket lui-meme, donc on supprime ses enfants un par un (sinon l'appel
    renvoie False alors que le contenu a bien ete efface).

    Retourne le nombre d'entrees supprimees.
    """
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(hadoop_conf)

    if not fs.exists(path):
        logger.info("Rien a supprimer : %s n'existe pas", uri)
        return 0

    is_bucket_root = path.getParent() is None
    if not is_bucket_root:
        deleted = bool(fs.delete(path, True))
        logger.info("Suppression de %s : %s", uri, "OK" if deleted else "echec")
        return 1 if deleted else 0

    count = 0
    for status in fs.listStatus(path):
        child = status.getPath()
        if fs.delete(child, True):
            count += 1
        else:
            logger.warning("Impossible de supprimer %s", child.toString())
    logger.info("Contenu de %s vide : %s entree(s) supprimee(s)", uri, count)
    return count
