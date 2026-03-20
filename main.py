from spark.utils import create_spark_session
from spark.io import read_bronze
import logging
import argparse
from spark.config import load_config
from ingestion.base_ingestion import read_csv_to_dataframe, write_bronze
from transformations.bronze_to_silver import (
    write_silver,
   transform,
   deduplicate,
)
from transformations.silver_to_gold import (
    customer_orders_summary,
    daily_revenue,
    top_customers,
    write_gold
)

logging.basicConfig(level=logging.INFO)


def run_bronze(spark, config):
    """Ingest CSV sources into the bronze layer (MinIO)."""
    for table_name, conf in config["bronze"]["sources"].items():
        logging.info(f"[BRONZE] Processing table={table_name}")
        try:
            df = read_csv_to_dataframe(spark, conf["path"])
            write_bronze(df, table_name)
        except Exception as e:
            logging.error(f"Error processing table {table_name}: {e}")
            raise


def run_silver(spark, config):
    """Transform bronze data and write to the silver layer."""
    for table_name, conf in config["silver"]["sources"].items():
        logging.info(f"[SILVER] Processing table={table_name}")
        try:
            df = read_bronze(spark, conf["path"])
            df = transform(df, conf.get("transformations"))
            df = deduplicate(df, conf["primary_key"], conf["order_by"])
            df.show(5)
            write_silver(df, table_name)
        except Exception as e:
            logging.error(f"Error processing table {table_name}: {e}")
            raise


def run_gold(spark):
    """Build gold aggregation tables from the silver layer."""
    try:
        customer_df = customer_orders_summary(
            spark,
            "s3a://silver/customers/",
            "s3a://silver/orders/",
        )
        write_gold(customer_df, "customer_orders_summary")

        daily_revenue_df = daily_revenue(
            spark,
            "s3a://silver/orders/",
        )
        write_gold(daily_revenue_df, "daily_revenue")

        top_customers_df = top_customers(
            spark,
            "s3a://silver/orders/",
            top_n=10,
        )
        write_gold(top_customers_df, "top_customers")
    except Exception as e:
        logging.error(f"Error processing gold layer: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lakehouse pipeline runner")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold"],
        help="Run a single layer. If omitted, all layers run sequentially.",
    )
    args = parser.parse_args()

    spark = create_spark_session("lakehouse_pipeline")
    config = load_config("/opt/project/config/settings.yaml")

    try:
        if args.layer == "bronze":
            run_bronze(spark, config)
        elif args.layer == "silver":
            run_silver(spark, config)
        elif args.layer == "gold":
            run_gold(spark)
        else:
            # No --layer flag: run all layers sequentially
            run_bronze(spark, config)
            run_silver(spark, config)
            run_gold(spark)
    finally:
        spark.stop()
