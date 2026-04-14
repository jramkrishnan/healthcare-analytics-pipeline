"""
healthcare_pipeline_dag.py
End-to-end healthcare analytics pipeline:
  1. Validate source CSV files exist
  2. Run data ingestion (raw schema load)
  3. Trigger dbt transformations
  4. Run dbt tests
  5. Notify on completion
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ── DAG defaults ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "healthcare_pipeline",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

SEED_DIR = Path("/opt/airflow/data/seed_data")
DBT_DIR  = Path("/opt/airflow/dbt")

REQUIRED_FILES = [
    "hospitals.csv",
    "patients.csv",
    "admissions.csv",
    "diagnoses.csv",
    "medicare_costs.csv",
]


# ── Task functions ────────────────────────────────────────────────────────────

def validate_source_files(**context) -> str:
    """Check all seed CSVs are present; branch to generate them if not."""
    missing = [f for f in REQUIRED_FILES if not (SEED_DIR / f).exists()]
    if missing:
        log.warning(f"Missing source files: {missing} — regenerating …")
        return "generate_seed_data"
    log.info("All source files present ✓")
    return "ingest_data"


def generate_seed_data(**context):
    """Run the data generation script to produce CSV seed files."""
    script = Path("/opt/airflow/scripts/generate_data.py")
    result = subprocess.run(
        ["python3", str(script)],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Data generation failed:\n{result.stderr}")
    log.info(result.stdout)


def ingest_data(**context):
    """Load CSV files into PostgreSQL raw schema."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.load_data import run
    run()


def check_row_counts(**context):
    """Post-ingestion quality gate — fail if any table is suspiciously empty."""
    import psycopg2

    conn = psycopg2.connect(
        host     = os.getenv("POSTGRES_HOST", "postgres"),
        dbname   = os.getenv("POSTGRES_DB",   "healthcare"),
        user     = os.getenv("POSTGRES_USER",  "postgres"),
        password = os.getenv("POSTGRES_PASSWORD", "postgres"),
    )

    thresholds = {
        "raw.hospitals":      10,
        "raw.patients":       100,
        "raw.admissions":     500,
        "raw.diagnoses":      500,
        "raw.medicare_costs": 10,
    }

    failed = []
    with conn.cursor() as cur:
        for table, min_rows in thresholds.items():
            cur.execute(f"SELECT count(*) FROM {table}")
            count = cur.fetchone()[0]
            log.info(f"  {table}: {count:,} rows")
            if count < min_rows:
                failed.append(f"{table} has only {count} rows (min {min_rows})")

    conn.close()

    if failed:
        raise ValueError(f"Quality gate failed:\n" + "\n".join(failed))

    log.info("✅  Row count quality gate passed")


def log_pipeline_success(**context):
    log.info(
        f"🎉  Pipeline run complete | "
        f"DAG: {context['dag'].dag_id} | "
        f"Run: {context['run_id']} | "
        f"Logical date: {context['logical_date']}"
    )


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id          = "healthcare_analytics_pipeline",
    default_args    = DEFAULT_ARGS,
    description     = "End-to-end healthcare data pipeline: ingest → dbt → test",
    schedule        = "0 6 * * *",   # 06:00 UTC daily
    start_date      = datetime(2024, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["healthcare", "dbt", "ingestion"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Step 1: validate or generate source files ─────────────────────────────
    validate = BranchPythonOperator(
        task_id         = "validate_source_files",
        python_callable = validate_source_files,
    )

    generate = PythonOperator(
        task_id         = "generate_seed_data",
        python_callable = generate_seed_data,
    )

    # ── Step 2: ingest raw data ───────────────────────────────────────────────
    ingest = PythonOperator(
        task_id         = "ingest_data",
        python_callable = ingest_data,
        trigger_rule    = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── Step 3: quality gate ──────────────────────────────────────────────────
    quality_gate = PythonOperator(
        task_id         = "check_row_counts",
        python_callable = check_row_counts,
    )

    # ── Step 4: dbt run ───────────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id     = "dbt_run",
        bash_command = (
            f"cd {DBT_DIR} && "
            "dbt run "
            "--profiles-dir /opt/airflow/dbt "
            "--project-dir /opt/airflow/dbt "
            "--select staging intermediate marts "
            "--vars '{\"run_date\": \"{{ ds }}\"}'"
        ),
        env = {
            "POSTGRES_HOST":     "{{ var.value.get('POSTGRES_HOST', 'postgres') }}",
            "POSTGRES_DB":       os.getenv("POSTGRES_DB",       "healthcare"),
            "POSTGRES_USER":     os.getenv("POSTGRES_USER",     "postgres"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
        },
    )

    # ── Step 5: dbt test ──────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id     = "dbt_test",
        bash_command = (
            f"cd {DBT_DIR} && "
            "dbt test "
            "--profiles-dir /opt/airflow/dbt "
            "--project-dir /opt/airflow/dbt "
            "--select marts"
        ),
    )

    # ── Step 6: dbt docs generate ────────────────────────────────────────────
    dbt_docs = BashOperator(
        task_id     = "dbt_docs_generate",
        bash_command = (
            f"cd {DBT_DIR} && "
            "dbt docs generate "
            "--profiles-dir /opt/airflow/dbt "
            "--project-dir /opt/airflow/dbt"
        ),
    )

    # ── Step 7: completion log ────────────────────────────────────────────────
    success_log = PythonOperator(
        task_id         = "log_success",
        python_callable = log_pipeline_success,
    )

    end = EmptyOperator(
        task_id      = "end",
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    start >> validate >> [generate, ingest]
    generate >> ingest
    ingest >> quality_gate >> dbt_run >> dbt_test >> dbt_docs >> success_log >> end
