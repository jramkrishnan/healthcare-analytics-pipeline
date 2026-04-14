"""
dbt_dag.py
Standalone DAG for running or re-running only the dbt layer.
Useful when source data hasn't changed but models need to be rebuilt
(e.g., after a schema migration or dbt model change).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner":            "healthcare_pipeline",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=3),
}

DBT_CMD_BASE = (
    "dbt {command} "
    "--profiles-dir /opt/airflow/dbt "
    "--project-dir  /opt/airflow/dbt "
    "{extra}"
)

with DAG(
    dag_id       = "dbt_only_run",
    default_args = DEFAULT_ARGS,
    description  = "Run dbt transformations independently of ingestion",
    schedule     = None,          # manual trigger only
    start_date   = datetime(2024, 1, 1),
    catchup      = False,
    tags         = ["dbt", "healthcare"],
) as dag:

    start = EmptyOperator(task_id="start")

    compile_models = BashOperator(
        task_id     = "dbt_compile",
        bash_command = DBT_CMD_BASE.format(command="compile", extra=""),
    )

    run_staging = BashOperator(
        task_id     = "dbt_run_staging",
        bash_command = DBT_CMD_BASE.format(
            command="run",
            extra="--select staging"
        ),
    )

    run_intermediate = BashOperator(
        task_id     = "dbt_run_intermediate",
        bash_command = DBT_CMD_BASE.format(
            command="run",
            extra="--select intermediate"
        ),
    )

    run_marts = BashOperator(
        task_id     = "dbt_run_marts",
        bash_command = DBT_CMD_BASE.format(
            command="run",
            extra="--select marts"
        ),
    )

    run_tests = BashOperator(
        task_id     = "dbt_test",
        bash_command = DBT_CMD_BASE.format(
            command="test",
            extra="--select marts"
        ),
    )

    generate_docs = BashOperator(
        task_id     = "dbt_docs",
        bash_command = DBT_CMD_BASE.format(
            command="docs generate",
            extra=""
        ),
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> compile_models
        >> run_staging
        >> run_intermediate
        >> run_marts
        >> run_tests
        >> generate_docs
        >> end
    )
