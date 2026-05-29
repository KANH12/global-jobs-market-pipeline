from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "khang",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

DATE_PATH = "{{ ds.replace('-', '/') }}"


with DAG(
    dag_id="adzuna_jobs_market_pipeline",
    description="Orchestrate Adzuna jobs market data pipeline",
    default_args=default_args,
    start_date=datetime(2026, 5, 29),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["adzuna", "jobs", "spark", "postgres"],
) as dag:

    ingestion = BashOperator(
        task_id="ingest_jobs_from_api",
        bash_command=(
            "docker exec spark "
            "python /home/jovyan/ingestion/run_ingestion.py"
        ),
    )

    processing = BashOperator(
        task_id="run_processing_pipeline",
        bash_command=(
            "docker exec spark "
            "spark-submit /home/jovyan/processing/run_processing_pipeline.py "
            f"--date {DATE_PATH}"
        ),
    )

    database = BashOperator(
        task_id="load_gold_to_postgres",
        bash_command=(
            "docker exec spark "
            "spark-submit /home/jovyan/storage/pipeline_database.py "
            f"--date {DATE_PATH}"
        ),
    )

    ingestion >> processing >> database