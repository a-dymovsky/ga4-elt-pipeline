"""
GA4 pipeline DAG: generate -> load -> dbt build, one run per day.

"""
from __future__ import annotations

import sys
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

# Make include/ importable regardless of how Airflow loads this file.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from include.generator.generate_events import generate
from include.loader.load_to_duckdb import load_partition

DBT_DIR = "/usr/local/airflow/include/dbt"


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    catchup=False,
    tags=["ga4", "dbt", "duckdb"],
)
def ga4_pipeline():
    @task
    def generate_events(**context) -> str:
        run_date = context["ds"]
        generate(date=run_date)
        return run_date

    @task
    def load(run_date: str) -> int:
        return load_partition(date=run_date)

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build",
    )

    day = generate_events()
    load(day) >> dbt_build


ga4_pipeline()
