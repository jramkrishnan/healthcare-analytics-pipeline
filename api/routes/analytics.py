"""
api/routes/analytics.py
Analytics endpoints backed by dbt mart tables in the analytics schema.
"""

from typing import List, Optional
from fastapi import APIRouter, Query, HTTPException

from api.models.db import database
from api.models.schemas import ReadmissionSummary, CostSummary, PatientSummary, PipelineRunSummary

router = APIRouter()


# ── Readmission ───────────────────────────────────────────────────────────────

@router.get(
    "/readmission",
    response_model=List[ReadmissionSummary],
    summary="Hospital readmission rates",
    description=(
        "Returns 30-day readmission rates aggregated at hospital × year × quarter level. "
        "Filter by state, year, or hospital type."
    ),
)
async def get_readmission(
    state:          Optional[str] = Query(None,  description="Two-letter state code, e.g. MA"),
    year:           Optional[int] = Query(None,  description="Admit year, e.g. 2022"),
    hospital_type:  Optional[str] = Query(None,  description="Acute Care | Teaching | Critical Access | Specialty"),
    min_rate:       float         = Query(0.0,   description="Minimum readmission rate %"),
    limit:          int           = Query(100,   ge=1, le=1000),
):
    where_clauses = ["1=1"]
    if state:
        where_clauses.append(f"hospital_state = '{state.upper()}'")
    if year:
        where_clauses.append(f"admit_year = {year}")
    if hospital_type:
        where_clauses.append(f"hospital_type ILIKE '%{hospital_type}%'")
    where_clauses.append(f"readmission_rate_pct >= {min_rate}")

    sql = f"""
        SELECT
            hospital_id, hospital_name, hospital_state, hospital_type,
            admit_year, total_admissions, readmissions, readmission_rate_pct,
            avg_los_days, avg_charges, avg_risk_score,
            high_risk_patients, icu_admissions, vs_national_avg
        FROM analytics.mart_readmission_analysis
        WHERE {" AND ".join(where_clauses)}
        ORDER BY readmission_rate_pct DESC
        LIMIT {limit}
    """
    rows = await database.fetch_all(sql)
    return [dict(r) for r in rows]


@router.get(
    "/readmission/summary",
    summary="National readmission summary statistics",
)
async def readmission_summary(year: Optional[int] = None):
    year_filter = f"WHERE admit_year = {year}" if year else ""
    sql = f"""
        SELECT
            admit_year,
            count(distinct hospital_id)                   AS hospitals,
            sum(total_admissions)                         AS total_admissions,
            sum(readmissions)                             AS total_readmissions,
            round(avg(readmission_rate_pct)::numeric, 2)  AS avg_readmission_rate,
            round(min(readmission_rate_pct)::numeric, 2)  AS min_rate,
            round(max(readmission_rate_pct)::numeric, 2)  AS max_rate,
            round(avg(avg_charges)::numeric, 2)           AS avg_charges_per_admission
        FROM analytics.mart_readmission_analysis
        {year_filter}
        GROUP BY admit_year
        ORDER BY admit_year
    """
    rows = await database.fetch_all(sql)
    return [dict(r) for r in rows]


# ── Cost ──────────────────────────────────────────────────────────────────────

@router.get(
    "/cost",
    response_model=List[CostSummary],
    summary="Medicare cost analysis by hospital and DRG",
)
async def get_cost(
    state:        Optional[str] = Query(None),
    drg_code:     Optional[str] = Query(None,  description="CMS DRG code, e.g. 470"),
    year:         Optional[int] = Query(None),
    flag:         Optional[str] = Query(None,  description="High Cost Outlier | Low Cost Outlier | Within Benchmark"),
    limit:        int           = Query(100,   ge=1, le=1000),
):
    where_clauses = ["1=1"]
    if state:
        where_clauses.append(f"state = '{state.upper()}'")
    if drg_code:
        where_clauses.append(f"drg_code = '{drg_code}'")
    if year:
        where_clauses.append(f"year = {year}")
    if flag:
        where_clauses.append(f"cost_efficiency_flag ILIKE '%{flag}%'")

    sql = f"""
        SELECT
            hospital_id, hospital_name, state, drg_code, drg_description,
            year, total_discharges, avg_covered_charges, avg_medicare_payments,
            medicare_payment_ratio, cost_payment_gap,
            charge_vs_national, cost_efficiency_flag, total_write_off_estimate
        FROM analytics.mart_cost_analysis
        WHERE {" AND ".join(where_clauses)}
        ORDER BY total_write_off_estimate DESC NULLS LAST
        LIMIT {limit}
    """
    rows = await database.fetch_all(sql)
    return [dict(r) for r in rows]


@router.get(
    "/cost/drg-benchmark",
    summary="National average costs per DRG code",
)
async def drg_benchmark(year: Optional[int] = None):
    year_filter = f"AND year = {year}" if year else ""
    sql = f"""
        SELECT
            drg_code,
            drg_description,
            year,
            count(distinct hospital_id)                           AS hospitals_reporting,
            round(avg(avg_covered_charges)::numeric, 2)           AS national_avg_charges,
            round(avg(avg_medicare_payments)::numeric, 2)         AS national_avg_medicare,
            round(avg(medicare_payment_ratio)::numeric, 4)        AS avg_payment_ratio,
            sum(total_discharges)                                  AS total_discharges
        FROM analytics.mart_cost_analysis
        WHERE 1=1 {year_filter}
        GROUP BY drg_code, drg_description, year
        ORDER BY total_discharges DESC
    """
    rows = await database.fetch_all(sql)
    return [dict(r) for r in rows]


# ── Patients ──────────────────────────────────────────────────────────────────

@router.get(
    "/patients",
    response_model=List[PatientSummary],
    summary="Patient-level summary with risk stratification",
)
async def get_patients(
    age_group:     Optional[str] = Query(None, description="Pediatric | Adult | Middle-Aged | Senior | Elderly"),
    insurance:     Optional[str] = Query(None),
    high_utiliser: Optional[bool] = Query(None),
    min_admissions: int          = Query(1),
    limit:          int          = Query(100, ge=1, le=1000),
):
    where_clauses = [f"total_admissions >= {min_admissions}"]
    if age_group:
        where_clauses.append(f"age_group ILIKE '%{age_group}%'")
    if insurance:
        where_clauses.append(f"insurance_type ILIKE '%{insurance}%'")
    if high_utiliser is not None:
        where_clauses.append(f"high_utiliser = {high_utiliser}")

    sql = f"""
        SELECT
            patient_id, age, age_group, gender, race, insurance_type,
            total_admissions, total_readmissions, personal_readmission_rate_pct,
            avg_los, total_lifetime_charges, avg_risk_score, high_utiliser
        FROM analytics.mart_patient_summary
        WHERE {" AND ".join(where_clauses)}
        ORDER BY avg_risk_score DESC
        LIMIT {limit}
    """
    rows = await database.fetch_all(sql)
    return [dict(r) for r in rows]


@router.get("/patients/demographics", summary="Demographic breakdown of patient cohort")
async def demographics():
    sql = """
        SELECT
            age_group,
            gender,
            race,
            insurance_type,
            count(*)                                           AS patients,
            round(avg(total_admissions)::numeric, 2)          AS avg_admissions,
            round(avg(personal_readmission_rate_pct)::numeric, 2) AS avg_readmission_rate,
            round(avg(avg_risk_score)::numeric, 2)            AS avg_risk_score,
            sum(high_utiliser::int)                           AS high_utilisers
        FROM analytics.mart_patient_summary
        GROUP BY age_group, gender, race, insurance_type
        ORDER BY patients DESC
    """
    rows = await database.fetch_all(sql)
    return [dict(r) for r in rows]


# ── Pipeline runs ─────────────────────────────────────────────────────────────

@router.get(
    "/pipeline-runs",
    response_model=List[PipelineRunSummary],
    summary="Recent pipeline execution history",
)
async def pipeline_runs(limit: int = Query(20, ge=1, le=100)):
    sql = f"""
        SELECT run_id, dag_id, status, rows_loaded,
               start_time::text, end_time::text
        FROM raw.pipeline_runs
        ORDER BY run_id DESC
        LIMIT {limit}
    """
    rows = await database.fetch_all(sql)
    return [dict(r) for r in rows]
