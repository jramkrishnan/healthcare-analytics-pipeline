"""
api/models/schemas.py
Pydantic models for API request/response validation.
"""

from typing import Optional, List
from pydantic import BaseModel, Field


class ReadmissionSummary(BaseModel):
    hospital_id:               str
    hospital_name:             str
    hospital_state:            str
    hospital_type:             str
    admit_year:                int
    total_admissions:          int
    readmissions:              int
    readmission_rate_pct:      float
    avg_los_days:              float
    avg_charges:               float
    avg_risk_score:            float
    high_risk_patients:        int
    icu_admissions:            int
    vs_national_avg:           Optional[float]


class CostSummary(BaseModel):
    hospital_id:               str
    hospital_name:             str
    state:                     str
    drg_code:                  str
    drg_description:           str
    year:                      int
    total_discharges:          int
    avg_covered_charges:       float
    avg_medicare_payments:     float
    medicare_payment_ratio:    float
    cost_payment_gap:          float
    charge_vs_national:        Optional[float]
    cost_efficiency_flag:      str
    total_write_off_estimate:  Optional[float]


class PatientSummary(BaseModel):
    patient_id:                    str
    age:                           int
    age_group:                     str
    gender:                        str
    race:                          str
    insurance_type:                str
    total_admissions:              int
    total_readmissions:            int
    personal_readmission_rate_pct: float
    avg_los:                       float
    total_lifetime_charges:        float
    avg_risk_score:                float
    high_utiliser:                 bool


class QueryRequest(BaseModel):
    question: str = Field(
        ...,
        example="Which hospitals had a readmission rate above 20% in 2022?",
        description="Natural language question about the healthcare dataset",
    )
    max_rows: int = Field(50, ge=1, le=500)


class QueryResponse(BaseModel):
    question:   str
    sql:        str
    results:    List[dict]
    row_count:  int
    model_used: str


class PipelineRunSummary(BaseModel):
    run_id:       int
    dag_id:       str
    status:       str
    rows_loaded:  Optional[int]
    start_time:   Optional[str]
    end_time:     Optional[str]
