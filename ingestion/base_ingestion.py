from pyspark.sql import SparkSession, DataFrame

def read_csv_to_dataframe(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Reads a CSV file and returns a Spark DataFrame.
    """
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df


def write_bronze(df: DataFrame, table_name: str):
    """
    Writes a Spark DataFrame to the bronze layer in MinIO (Parquet)
    and registers it as an external Hive table so dbt can query it.
    """
    path = f"s3a://bronze/{table_name}/"
    df.write.mode("overwrite").parquet(path)

    # Récupérer la SparkSession active pour enregistrer la table dans le metastore
    spark = SparkSession.getActiveSession()
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze.{table_name}
        USING PARQUET
        LOCATION '{path}'
    """)
    # Rafraîchir les métadonnées après overwrite
    spark.sql(f"REFRESH TABLE bronze.{table_name}")


