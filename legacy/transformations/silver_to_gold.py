import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, count, sum as _sum, max as _max,
    dense_rank, to_date, coalesce, lit,
)
from spark.io import read_silver

logger = logging.getLogger(__name__)


def customer_orders_summary(
    spark: SparkSession,
    silver_customers_path: str,
    silver_orders_path: str,
) -> DataFrame:
    """
    Builds an enriched customer view by joining customers and orders from the silver layer,
    then aggregating order metrics per customer.

    Returns:
        DataFrame with columns: customer_id, email, total_orders, total_amount, last_order_date
    """
    logger.info("Building customer_orders_summary …")

    df_customers = read_silver(spark, silver_customers_path)
    df_orders = (
        read_silver(spark, silver_orders_path)
        .filter(col("status") == "shipped")
    )

    orders_agg = df_orders.groupBy("customer_id").agg(
        count("order_id").alias("total_orders"),
        _sum("amount").alias("total_amount"),
        _max("timestamp").alias("order_date"),
    )

    gold_df = (
        df_customers
        .join(orders_agg, on="customer_id", how="left")
        .select(
            col("customer_id"),
            col("email"),
            coalesce(col("total_orders"), lit(0)).alias("total_orders"),
            coalesce(col("total_amount"), lit(0)).alias("total_amount"),
            col("order_date"),
        )
    )

    logger.info("customer_orders_summary built successfully.")
    return gold_df


def daily_revenue(
    spark: SparkSession,
    silver_orders_path: str,
) -> DataFrame:
    """
    Builds a daily revenue summary by aggregating orders from the silver layer.

    Returns:
        DataFrame with columns: order_date, total_revenue, total_orders
    """
    logger.info("Building daily_revenue …")

    df_orders = (
        read_silver(spark, silver_orders_path)
        .filter(col("status")  == "shipped")
    )

    df_daily_revenue = (
        df_orders
        .withColumn("order_date", to_date(col("timestamp")))
        .groupBy("order_date")
        .agg(
            _sum("amount").alias("total_revenue"),
            count("order_id").alias("total_orders"),
        )
        .orderBy("order_date")
    )

    return df_daily_revenue


def top_customers(
    spark: SparkSession,
    silver_orders_path: str,
    top_n: int = 10,
) -> DataFrame:
    """
    Ranks customers by total amount spent (descending) and returns the top N.

    Returns:
        DataFrame with columns: customer_id, total_amount, rank
    """
    logger.info("Building top_customers (top %d) …", top_n)

    df_orders = (
        read_silver(spark, silver_orders_path)
        .filter(col("status")  == "shipped")
    )

    customers_agg = df_orders.groupBy("customer_id").agg(
        _sum("amount").alias("total_amount"),
    )

    window_spec = Window.orderBy(col("total_amount").desc())
    ranked = customers_agg.withColumn("rank", dense_rank().over(window_spec))

    return ranked.filter(col("rank") <= top_n).select("customer_id", "total_amount", "rank")


def write_gold(df: DataFrame, table_name: str) -> None:
    """Writes a Spark DataFrame to the gold layer in Minio."""
    logger.info("Writing gold table: %s", table_name)
    df.write.mode("overwrite").parquet(f"s3a://gold/{table_name}/")
