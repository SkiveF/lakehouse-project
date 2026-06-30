from pyspark.sql.functions import col, lower, row_number
from pyspark.sql.window import Window


def transform(df, transformations):
    """
    Transforms a Spark DataFrame from the bronze layer to the silver layer based on the provided transformations.
    """
    if not transformations:
       return df

    for t in transformations:
        if t == "lowercase_email":
            df = df.withColumn("email", lower(col("email")))
    return df

def deduplicate(df, primary_key, order_by):
    """
    Deduplicates a Spark DataFrame based on the specified primary key and order by column.
    """
    window_spec = Window.partitionBy(primary_key).orderBy(col(order_by).desc())
    return (
        df.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

def write_silver( df, table_name, ):
    """
    Writes a Spark DataFrame to the silver layer in Minio.
    """
    df.write.mode("overwrite").parquet(f"s3a://silver/{table_name}/")

