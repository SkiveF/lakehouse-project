from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

SPARK_SUBMIT = (
    "docker exec spark "
    "/opt/spark/bin/spark-submit /opt/project/main.py"
)

DBT_RUN = (
    "docker exec dbt dbt run "
    "--profiles-dir /opt/project/lakehouse_dbt "
    "--project-dir /opt/project/lakehouse_dbt"
)

REFRESH_BRONZE = (
    "docker exec spark-thrift /opt/spark/bin/beeline "
    "-u 'jdbc:hive2://localhost:10000' --silent=true "
    "-e \"REFRESH TABLE bronze.customers; REFRESH TABLE bronze.orders;\""
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

    bronze >> refresh_bronze >> dbt
