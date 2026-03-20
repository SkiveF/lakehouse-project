from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

SPARK_SUBMIT = (
    "docker exec spark "
    "/opt/spark/bin/spark-submit /opt/project/main.py"
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
    tags=["lakehouse", "spark", "minio"],
) as dag:

    bronze = BashOperator(
        task_id="bronze",
        bash_command=f"{SPARK_SUBMIT} --layer bronze",
    )

    silver = BashOperator(
        task_id="silver",
        bash_command=f"{SPARK_SUBMIT} --layer silver",
    )

    gold = BashOperator(
        task_id="gold",
        bash_command=f"{SPARK_SUBMIT} --layer gold",
    )

    bronze >> silver >> gold