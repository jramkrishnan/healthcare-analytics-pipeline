"""
Healthcare Pipeline — Data Ingestion
Loads CSV seed data into raw PostgreSQL schema with basic validation,
idempotent upserts, and run logging.
"""

import os
import csv
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Connection ────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "healthcare"),
    "user":     os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

SEED_DIR = Path(__file__).parent.parent / "data" / "seed_data"


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_csv(path: Path) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def cast_row(row: dict, bool_cols=(), int_cols=(), float_cols=()) -> dict:
    """Type-cast raw CSV strings to Python native types."""
    for col in bool_cols:
        if col in row:
            row[col] = row[col].strip().lower() in ("true", "1", "yes")
    for col in int_cols:
        if col in row and row[col] not in ("", None):
            row[col] = int(row[col])
        elif col in row:
            row[col] = None
    for col in float_cols:
        if col in row and row[col] not in ("", None):
            row[col] = float(row[col])
        elif col in row:
            row[col] = None
    return row


def upsert(conn, table: str, rows: list[dict], pk: str) -> int:
    if not rows:
        return 0
    cols   = list(rows[0].keys())
    values = [tuple(r[c] for c in cols) for r in rows]
    update = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != pk)
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({pk}) DO UPDATE SET {update}"
    )
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    return len(rows)


# ── Per-table loaders ─────────────────────────────────────────────────────────

def ingest_hospitals(conn) -> int:
    rows = load_csv(SEED_DIR / "hospitals.csv")
    rows = [cast_row(r, bool_cols=("teaching_flag",), int_cols=("bed_count",)) for r in rows]
    return upsert(conn, "raw.hospitals", rows, "hospital_id")


def ingest_patients(conn) -> int:
    rows = load_csv(SEED_DIR / "patients.csv")
    rows = [cast_row(r, int_cols=("age",)) for r in rows]
    return upsert(conn, "raw.patients", rows, "patient_id")


def ingest_admissions(conn) -> int:
    rows = load_csv(SEED_DIR / "admissions.csv")
    rows = [
        cast_row(
            r,
            bool_cols=("readmitted_30_days",),
            int_cols=("los_days", "icu_hours"),
            float_cols=("total_charges",),
        )
        for r in rows
    ]
    # Remove loaded_at if present so DB default applies
    for r in rows:
        r.pop("loaded_at", None)
    return upsert(conn, "raw.admissions", rows, "admission_id")


def ingest_diagnoses(conn) -> int:
    rows = load_csv(SEED_DIR / "diagnoses.csv")
    rows = [cast_row(r, int_cols=("diagnosis_rank",)) for r in rows]
    return upsert(conn, "raw.diagnoses", rows, "diagnosis_id")


def ingest_medicare_costs(conn) -> int:
    rows = load_csv(SEED_DIR / "medicare_costs.csv")
    rows = [
        cast_row(
            r,
            int_cols=("total_discharges", "year"),
            float_cols=("avg_covered_charges", "avg_total_payments", "avg_medicare_payments"),
        )
        for r in rows
    ]
    return upsert(conn, "raw.medicare_costs", rows, "cost_id")


# ── Pipeline run logging ──────────────────────────────────────────────────────

def log_run(conn, dag_id: str, start: datetime, rows: int, status: str, error: str = None):
    sql = """
        INSERT INTO raw.pipeline_runs (dag_id, run_type, start_time, end_time, rows_loaded, status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (dag_id, "full_load", start, datetime.utcnow(), rows, status, error))
    conn.commit()


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    start  = datetime.utcnow()
    total  = 0
    conn   = None
    status = "success"
    error  = None

    try:
        log.info("Connecting to PostgreSQL …")
        conn = psycopg2.connect(**DB_CONFIG)

        steps = [
            ("hospitals",      ingest_hospitals),
            ("patients",       ingest_patients),
            ("admissions",     ingest_admissions),
            ("diagnoses",      ingest_diagnoses),
            ("medicare_costs", ingest_medicare_costs),
        ]

        for name, fn in steps:
            log.info(f"  Loading {name} …")
            n = fn(conn)
            log.info(f"    ↳  {n:,} rows upserted")
            total += n

        log.info(f"\n✅  Ingestion complete — {total:,} rows total")

    except Exception as exc:
        status = "failed"
        error  = str(exc)
        log.error(f"❌  Ingestion failed: {exc}")
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            log_run(conn, "ingestion_dag", start, total, status, error)
            conn.close()


if __name__ == "__main__":
    run()
