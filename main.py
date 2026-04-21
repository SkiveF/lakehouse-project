from spark.utils import create_spark_session
import logging
from spark.config import load_config
from ingestion.base_ingestion import read_csv_to_dataframe, write_bronze

logging.basicConfig(level=logging.INFO)


def run_bronze(spark, config):
    """Ingest CSV sources into the bronze layer (MinIO).
    Silver and Gold are handled by dbt (lakehouse_dbt/).
    """
    for table_name, conf in config["bronze"]["sources"].items():
        logging.info(f"[BRONZE] Processing table={table_name}")
        try:
            df = read_csv_to_dataframe(spark, conf["path"])
            write_bronze(df, table_name)
        except Exception as e:
            logging.error(f"Error processing table {table_name}: {e}")
            raise


if __name__ == "__main__":
    spark = create_spark_session("lakehouse_pipeline")
    config = load_config("/opt/project/config/settings.yaml")
    try:
        run_bronze(spark, config)
        logging.info("✅ Bronze ingestion done. Run 'dbt run' to process Silver & Gold.")
    finally:
        spark.stop()
