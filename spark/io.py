

def read_bronze(spark, path):
    """
    Reads a Spark DataFrame from the bronze layer in Minio based on the provided table name.
    """
    return spark.read.parquet(path)


def read_silver(spark, path):
    """
    Reads a Spark DataFrame from the silver layer in Minio based on the provided table name.
    """
    return spark.read.parquet(path)