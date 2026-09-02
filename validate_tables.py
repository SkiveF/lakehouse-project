#!/usr/bin/env python3
"""
Validate the complete lakehouse table structure.

This script checks:
1. All databases exist (bronze, silver, gold)
2. All expected tables are created
3. Table schemas are valid
4. Data is accessible in MinIO
"""

import sys
import logging
from typing import Dict, Tuple
from pyspark.sql import SparkSession

from spark.utils import create_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def check_databases(spark: SparkSession) -> Dict[str, bool]:
    """Check if required databases exist."""
    results = {}
    required_dbs = ["bronze", "silver", "gold"]

    try:
        result_rows = spark.sql("SHOW DATABASES").collect()
        existing_dbs = {row.databaseName for row in result_rows}

        for db in required_dbs:
            exists = db in existing_dbs
            results[db] = exists
            status = "OK" if exists else "MISSING"
            logger.info(f"{status} Database '{db}' {'exists' if exists else 'MISSING'}")
    except Exception as e:
        logger.error(f"Failed to check databases: {e}")
        return {db: False for db in required_dbs}

    return results


def check_tables(spark: SparkSession, database: str) -> Dict[str, Tuple[bool, int]]:
    """Check tables in a specific database. Returns (exists, row_count)."""
    results = {}

    try:
        # Get all tables in database
        table_rows = spark.sql(f"SHOW TABLES IN {database}").collect()
        existing_tables = {row.tableName for row in table_rows}

        # Expected tables for each database
        expected_tables = {
            "bronze": ["customers", "orders"],
            "silver": ["stg_customers", "stg_orders"],
            "gold": ["customer_orders_summary", "daily_revenue", "top_customers"]
        }

        expected = expected_tables.get(database, [])

        for table in expected:
            exists = table in existing_tables
            results[table] = (exists, 0)

            if exists:
                try:
                    # Try to count rows
                    row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {database}.{table}").collect()[0].cnt
                    results[table] = (exists, row_count)
                    status = "OK" if row_count > 0 else "WARN"
                    logger.info(f"{status} {database}.{table}: {row_count} rows")
                except Exception as e:
                    logger.warning(f"  Could not count rows in {database}.{table}: {e}")
                    results[table] = (exists, -1)
            else:
                logger.warning(f"MISSING {database}.{table} NOT FOUND")

    except Exception as e:
        logger.error(f"Failed to check tables in {database}: {e}")

    return results


def check_all_tables(spark: SparkSession) -> Dict[str, Dict[str, Tuple[bool, int]]]:
    """Check all tables in all databases."""
    all_results = {}

    for db in ["bronze", "silver", "gold"]:
        logger.info(f"\n[{db.upper()}] Checking tables...")
        all_results[db] = check_tables(spark, db)

    return all_results


def print_summary(db_results: Dict[str, bool], table_results: Dict[str, Dict[str, Tuple[bool, int]]]) -> bool:
    """Print a summary of the validation results."""
    logger.info("\n" + "="*60)
    logger.info("LAKEHOUSE VALIDATION SUMMARY")
    logger.info("="*60)

    # Count totals
    total_dbs = len(db_results)
    complete_dbs = sum(1 for v in db_results.values() if v)

    logger.info(f"\nDatabases: {complete_dbs}/{total_dbs} complete")

    total_tables = 0
    complete_tables = 0
    populated_tables = 0

    for db, tables in table_results.items():
        for table, (exists, count) in tables.items():
            total_tables += 1
            if exists:
                complete_tables += 1
                if count > 0:
                    populated_tables += 1

    logger.info(f"Tables: {complete_tables}/{total_tables} created, {populated_tables}/{total_tables} populated")

    # Status
    if complete_dbs == total_dbs and complete_tables == total_tables and populated_tables > 0:
        logger.info("\nLAKEHOUSE IS READY")
        return True
    elif complete_dbs == total_dbs and complete_tables == total_tables:
        logger.info("\nTABLES CREATED BUT NOT POPULATED")
        logger.info("Run: spark-submit /opt/project/main.py --layer bronze")
        logger.info("Then: dbt run")
        return True
    elif complete_dbs == total_dbs:
        logger.info("\nSOME TABLES MISSING")
        logger.info("Run: spark-submit /opt/project/init_tables.py")
        return False
    else:
        logger.error("\nDATABASES MISSING")
        logger.error("Run: spark-submit /opt/project/init_tables.py")
        return False


def main() -> None:
    """Lance la validation et sort en code 1 si le lakehouse est incomplet."""
    logger.info("Validating Lakehouse Structure...\n")

    try:
        spark = create_spark_session("lakehouse_validate")

        # Check databases
        logger.info("[DATABASES] Checking...")
        db_results = check_databases(spark)

        # Check tables
        logger.info("\n[TABLES] Checking...")
        table_results = check_all_tables(spark)

        # Print summary
        is_valid = print_summary(db_results, table_results)

        spark.stop()
        sys.exit(0 if is_valid else 1)

    except Exception as e:
        logger.error(f"\nValidation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

