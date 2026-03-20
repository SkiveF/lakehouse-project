from pyspark.sql import SparkSession, DataFrame

def read_csv_to_dataframe(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Reads a CSV file and returns a Spark DataFrame.

    Args:
        spark (SparkSession): The SparkSession object.
        file_path (str): The path to the CSV file to be read.

    Returns:
        DataFrame: A Spark DataFrame containing the data from the CSV file.
    """
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df


def write_bronze(df: DataFrame, table_name: str):
    """Writes a Spark DataFrame to the bronze layer in Minio."""
    df.write.mode("overwrite").parquet(f"s3a://bronze/{table_name}/")


