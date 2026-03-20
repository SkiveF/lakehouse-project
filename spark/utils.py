from pyspark.sql import SparkSession, DataFrame


def create_spark_session(app_name: str) -> SparkSession:
    """
    Creates and returns a SparkSession object.

    Args:
        app_name (str): The name of the Spark application.
    Returns:
        SparkSession: A SparkSession object for interacting with Spark.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()


