-- =============================================================================
-- Healthcare Analytics Pipeline — Database Schema
-- =============================================================================

-- Raw / source schema (mirrors incoming data, no transformations)
CREATE SCHEMA IF NOT EXISTS raw;

-- Analytics schema (dbt-managed marts land here)
CREATE SCHEMA IF NOT EXISTS analytics;

-- Staging schema (dbt intermediate models)
CREATE SCHEMA IF NOT EXISTS staging;


-- =============================================================================
-- RAW TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.hospitals (
    hospital_id     VARCHAR(10)  PRIMARY KEY,
    hospital_name   VARCHAR(200) NOT NULL,
    state           CHAR(2)      NOT NULL,
    city            VARCHAR(100),
    bed_count       INTEGER,
    hospital_type   VARCHAR(50),
    teaching_flag   BOOLEAN      DEFAULT FALSE,
    loaded_at       TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.patients (
    patient_id      VARCHAR(20)  PRIMARY KEY,
    age             INTEGER      CHECK (age BETWEEN 0 AND 120),
    gender          CHAR(1)      CHECK (gender IN ('M','F','U')),
    race            VARCHAR(50),
    insurance_type  VARCHAR(50),
    zip_code        VARCHAR(10),
    state           CHAR(2),
    loaded_at       TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.admissions (
    admission_id              VARCHAR(20)  PRIMARY KEY,
    patient_id                VARCHAR(20)  REFERENCES raw.patients(patient_id),
    hospital_id               VARCHAR(10)  REFERENCES raw.hospitals(hospital_id),
    admit_date                DATE         NOT NULL,
    discharge_date            DATE,
    admission_type            VARCHAR(30),
    primary_diagnosis_code    VARCHAR(10),
    primary_diagnosis_desc    VARCHAR(200),
    los_days                  INTEGER,
    readmitted_30_days        BOOLEAN      DEFAULT FALSE,
    discharge_disposition     VARCHAR(50),
    total_charges             NUMERIC(12,2),
    icu_hours                 INTEGER      DEFAULT 0,
    loaded_at                 TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.diagnoses (
    diagnosis_id    VARCHAR(20)  PRIMARY KEY,
    admission_id    VARCHAR(20)  REFERENCES raw.admissions(admission_id),
    icd_code        VARCHAR(10)  NOT NULL,
    icd_description VARCHAR(200),
    diagnosis_rank  INTEGER,
    diagnosis_type  VARCHAR(30),
    loaded_at       TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.medicare_costs (
    cost_id                 VARCHAR(20)   PRIMARY KEY,
    hospital_id             VARCHAR(10)   REFERENCES raw.hospitals(hospital_id),
    drg_code                VARCHAR(10),
    drg_description         VARCHAR(200),
    total_discharges        INTEGER,
    avg_covered_charges     NUMERIC(14,2),
    avg_total_payments      NUMERIC(14,2),
    avg_medicare_payments   NUMERIC(14,2),
    year                    INTEGER,
    loaded_at               TIMESTAMP     DEFAULT NOW()
);


-- =============================================================================
-- INDEXES — keep analytical queries fast
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_admissions_patient   ON raw.admissions(patient_id);
CREATE INDEX IF NOT EXISTS idx_admissions_hospital  ON raw.admissions(hospital_id);
CREATE INDEX IF NOT EXISTS idx_admissions_admit_date ON raw.admissions(admit_date);
CREATE INDEX IF NOT EXISTS idx_admissions_readmit    ON raw.admissions(readmitted_30_days);
CREATE INDEX IF NOT EXISTS idx_diagnoses_admission   ON raw.diagnoses(admission_id);
CREATE INDEX IF NOT EXISTS idx_diagnoses_icd         ON raw.diagnoses(icd_code);
CREATE INDEX IF NOT EXISTS idx_costs_hospital        ON raw.medicare_costs(hospital_id);
CREATE INDEX IF NOT EXISTS idx_costs_drg             ON raw.medicare_costs(drg_code);


-- =============================================================================
-- PIPELINE AUDIT TABLE — tracks each DAG run
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.pipeline_runs (
    run_id          SERIAL        PRIMARY KEY,
    dag_id          VARCHAR(100),
    run_type        VARCHAR(50),
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    rows_loaded     INTEGER,
    status          VARCHAR(20)   DEFAULT 'running',
    error_message   TEXT,
    created_at      TIMESTAMP     DEFAULT NOW()
);
