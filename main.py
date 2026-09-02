import argparse
import logging
from pathlib import Path

from ingestion.base_ingestion import read_csv_to_dataframe, write_bronze
from spark.config import load_config
from spark.utils import create_spark_session

logging.basicConfig(level=logging.INFO)


def parse_args():
    parser = argparse.ArgumentParser(description="Run lakehouse pipeline layers.")
    parser.add_argument(
        "--layer",
        choices=["bronze"],
        default="bronze",
        help="Pipeline layer to run. Silver and Gold are handled by dbt.",
    )
    return parser.parse_args()


def run_bronze(spark, config):
    """Ingest CSV sources into the bronze layer (MinIO).
    Silver and Gold are handled by dbt (lakehouse_dbt/).
    """
    for table_name, conf in config["bronze"]["sources"].items():
        logging.info("[BRONZE] Processing table=%s", table_name)
        try:
            df = read_csv_to_dataframe(spark, conf["path"])
            write_bronze(df, table_name)
        except Exception as e:
            logging.error("Error processing table %s: %s", table_name, e)
            raise


if __name__ == "__main__":
    args = parse_args()
    spark = create_spark_session("lakehouse_pipeline")
    project_root = Path(__file__).resolve().parent
    config = load_config(str(project_root / "config" / "settings.yaml"))
    try:
        if args.layer == "bronze":
            run_bronze(spark, config)
            logging.info("Bronze ingestion done. Run 'dbt run' to process Silver & Gold.")
    finally:
        spark.stop()
