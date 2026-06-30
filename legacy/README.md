# Legacy Spark Transformations

This folder keeps the previous Python/Spark implementation of the Silver and
Gold transformations for reference.

The active pipeline no longer imports these modules. The current execution path
is:

1. `main.py` ingests CSV files into the Bronze layer with Spark.
2. dbt models in `lakehouse_dbt/models/silver` build the Silver layer.
3. dbt models in `lakehouse_dbt/models/gold` build the Gold layer.

Keep these files only as historical reference while the dbt implementation is
the source of truth.
