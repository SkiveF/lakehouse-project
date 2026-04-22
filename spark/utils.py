import os
from pyspark.sql import SparkSession, DataFrame


def create_spark_session(app_name: str) -> SparkSession:
    """
    Creates and returns a SparkSession object.
    Credentials are read from environment variables (set via docker/.env).

    Args:
        app_name (str): The name of the Spark application.
    Returns:
        SparkSession: A SparkSession object for interacting with Spark.
    """
    s3a_access_key = os.environ.get("SPARK_S3A_ACCESS_KEY")
    s3a_secret_key = os.environ.get("SPARK_S3A_SECRET_KEY")

    if not s3a_access_key or not s3a_secret_key:
        raise EnvironmentError(
            "Missing S3A credentials. "
            "Set SPARK_S3A_ACCESS_KEY and SPARK_S3A_SECRET_KEY in docker/.env"
        )

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", s3a_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", s3a_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .enableHiveSupport() \
        .getOrCreate()


