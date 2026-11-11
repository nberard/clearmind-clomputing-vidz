"""grid_elt: every 15 minutes, load new grid metrics into BigQuery and rebuild
the dbt models (staging -> stage throughput -> scaling curve).

Config: environment variables (set in docker-compose / .env). The extract
task also accepts Airflow Variables with the same names.
  GRID_SERVER, GRID_TOKEN, GCP_PROJECT, BQ_RAW_DATASET (grid_raw),
  BQ_DATASET (grid_analytics), BQ_LOCATION (EU)
"""

import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.environ.get("VIBEGEN_ROOT", "/opt/vibegen")
DBT_DIR = f"{PROJECT_ROOT}/dbt"
DBT_BIN = os.environ.get("DBT_BIN", "dbt")  # dbt lives in its own venv in the Docker image


def cfg(name: str, default: str | None = None) -> str | None:
    return Variable.get(name, default_var=None) or os.environ.get(name, default)


@dag(
    dag_id="grid_elt",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["vibegen", "bigquery", "dbt"],
    doc_md=__doc__,
)
def grid_elt():
    @task
    def extract_load() -> dict:
        sys.path.insert(0, f"{PROJECT_ROOT}/elt")
        import extract

        target = extract.make_target(
            "bigquery",
            project=cfg("GCP_PROJECT"),
            dataset=cfg("BQ_RAW_DATASET", "grid_raw"),
            location=cfg("BQ_LOCATION", "EU"),
        )
        return extract.run(cfg("GRID_SERVER"), cfg("GRID_TOKEN"), target)

    @task.short_circuit
    def has_new_rows(loaded: dict) -> bool:
        # skip dbt when nothing new arrived
        return sum(loaded.values()) > 0

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} build --profiles-dir . --target bigquery",
        # from the container environment (cheap, no Airflow DB call when the DAG is parsed)
        env={k: os.environ.get(k, d) for k, d in {
            "GCP_PROJECT": "", "BQ_DATASET": "grid_analytics", "BQ_RAW_DATASET": "grid_raw",
            "BQ_LOCATION": "EU", "BQ_AUTH_METHOD": "service-account",
            "GOOGLE_APPLICATION_CREDENTIALS": "", "PATH": "/usr/bin:/bin",
        }.items()},
    )

    has_new_rows(extract_load()) >> dbt_build


grid_elt()
