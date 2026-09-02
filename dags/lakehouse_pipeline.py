from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Get environment variables with defaults
SPARK_THRIFT_HOST = os.getenv("DBT_SPARK_HOST", "spark-thrift")
SPARK_THRIFT_PORT = os.getenv("DBT_SPARK_PORT", "10000")

INIT_TABLES = (
    "docker exec spark "
    "/opt/spark/bin/spark-submit /opt/project/init_tables.py"
)

SPARK_SUBMIT = (
    "docker exec spark "
    "/opt/spark/bin/spark-submit /opt/project/main.py"
)

DBT_RUN = (
    "docker exec dbt dbt run "
    "--profiles-dir /opt/project/lakehouse_dbt "
    "--project-dir /opt/project/lakehouse_dbt"
)

# Build JDBC connection string from environment variables
REFRESH_BRONZE = (
    f"docker exec spark-thrift /opt/spark/bin/beeline "
    f"-u 'jdbc:hive2://{SPARK_THRIFT_HOST}:{SPARK_THRIFT_PORT}' --silent=true "
    f"-e \"REFRESH TABLE bronze.customers; REFRESH TABLE bronze.orders;\""
)

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lakehouse_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False,
    tags=["lakehouse", "spark", "dbt", "minio"],
) as dag:

    init = BashOperator(
        task_id="init_tables",
        bash_command=INIT_TABLES,
    )

    bronze = BashOperator(
        task_id="bronze",
        bash_command=f"{SPARK_SUBMIT} --layer bronze",
    )

    refresh_bronze = BashOperator(
        task_id="refresh_bronze",
        bash_command=REFRESH_BRONZE,
    )

    dbt = BashOperator(
        task_id="dbt_run",
        bash_command=DBT_RUN,
    )

    init >> bronze >> refresh_bronze >> dbt
