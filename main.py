"""Point d'entree de l'ingestion Bronze.

Silver et Gold sont construits par dbt (lakehouse_dbt/).

Exemples :
    spark-submit /opt/project/main.py --layer bronze
    spark-submit /opt/project/main.py --layer bronze --ingestion-date 2026-09-01
    spark-submit /opt/project/main.py --layer bronze --only orders
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timezone
from pathlib import Path

from ingestion.base_ingestion import read_source_to_dataframe, write_bronze
from spark.config import load_config, validate_bronze_config
from spark.utils import create_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingestion des sources vers Bronze.")
    parser.add_argument(
        "--layer",
        choices=["bronze"],
        default="bronze",
        help="Couche a executer. Silver et Gold sont geres par dbt.",
    )
    parser.add_argument(
        "--ingestion-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Date de partition a ecrire (defaut : aujourd'hui, UTC). "
             "Utile pour rejouer une ingestion.",
    )
    parser.add_argument(
        "--only",
        default=None,
        metavar="TABLE",
        help="N'ingerer qu'une seule source (nom tel que declare dans settings.yaml).",
    )
    return parser.parse_args()


def resolve_ingestion_date(raw: str | None) -> date:
    if raw is None:
        return datetime.now(timezone.utc).date()
    try:
        return date.fromisoformat(raw)
    except ValueError as error:
        raise SystemExit(f"--ingestion-date invalide ({raw}) : {error}") from error


def run_bronze(spark, sources: dict, root: str, ingestion_date: date, only: str | None) -> None:
    """Ingere chaque source declaree dans la couche Bronze."""
    if only:
        if only not in sources:
            raise SystemExit(
                f"Source '{only}' inconnue. Disponibles : {sorted(sources)}"
            )
        sources = {only: sources[only]}

    ingested_at = datetime.now(timezone.utc)
    total = 0

    for table_name, source_conf in sources.items():
        logger.info("[BRONZE] table=%s", table_name)
        df = read_source_to_dataframe(spark, source_conf)
        total += write_bronze(
            df,
            table_name=table_name,
            root=root,
            ingested_at=ingested_at,
            ingestion_date=ingestion_date,
        )

    logger.info(
        "[BRONZE] termine : %s lignes sur %s table(s), partition %s",
        total, len(sources), ingestion_date.isoformat(),
    )


def main() -> None:
    args = parse_args()
    ingestion_date = resolve_ingestion_date(args.ingestion_date)

    project_root = Path(__file__).resolve().parent
    config = load_config(str(project_root / "config" / "settings.yaml"))
    sources = validate_bronze_config(config)
    root = config["bronze"].get("root", "s3a://bronze")

    spark = create_spark_session("lakehouse_bronze_ingestion")
    try:
        run_bronze(spark, sources, root, ingestion_date, args.only)
        logger.info("Ingestion Bronze terminee. Lance 'dbt run' pour Silver & Gold.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
