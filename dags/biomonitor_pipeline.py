from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import os

# Define project root path
PROJECT_DIR = "/workspaces/biomonitor-capstone"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'biomonitor_pipeline',
    default_args=default_args,
    description='Full pipeline: Ingest GBIF data (dlt) and transform it (dbt)',
    schedule=None,  # Manual trigger only to avoid 'ghost' runs
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['biomonitor', 'ingestion', 'dbt'],
) as dag:

    # Task 1: dlt ingestion
    ingest_task = BashOperator(
        task_id='ingest_gbif_data',
        bash_command='cd /workspaces/biomonitor-capstone/ingestion && /workspaces/biomonitor-capstone/.venv/bin/python gbif_ingestion.py',
        env={'PYTHONUNBUFFERED': '1'},
    )

    # Task 2: dbt transformation
    dbt_task = BashOperator(
        task_id='dbt_transformation',
        bash_command=f'cd {PROJECT_DIR}/dbt_transformation && uv run dbt build',
    )

    # define the flow
    ingest_task >> dbt_task
