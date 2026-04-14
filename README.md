# 🏥 Healthcare Analytics Pipeline

An end-to-end healthcare data engineering project that demonstrates a modern data stack for hospital readmission analysis, Medicare cost benchmarking, and patient risk stratification — with a privacy-first Text-to-SQL query interface powered by a locally-hosted LLM.

---

## Architecture

```
CSV Seed Data
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│                      Docker Compose                          │
│                                                             │
│  ┌──────────┐    ┌──────────────────┐    ┌──────────────┐  │
│  │ Airflow  │───▶│   PostgreSQL 16  │◀───│   FastAPI    │  │
│  │ DAG      │    │                  │    │   + LangChain│  │
│  │ (ingest  │    │  raw.*  (source) │    │              │  │
│  │  + dbt)  │    │  staging.*       │    └──────┬───────┘  │
│  └──────────┘    │  analytics.*     │           │          │
│                  │  (dbt marts)     │    ┌──────▼───────┐  │
│                  └──────────────────┘    │    Ollama    │  │
│                                          │  (llama3)    │  │
│                                          │  local only  │  │
│                                          └──────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Storage | PostgreSQL 16 |
| Transformation | dbt-core 1.8 + dbt-postgres |
| API | FastAPI + asyncpg |
| LLM Inference | Ollama (llama3 — local) |
| NL-to-SQL | LangChain |
| Containerisation | Docker Compose |

---

## Project Structure

```
healthcare-analytics-pipeline/
├── airflow/
│   └── dags/
│       ├── healthcare_pipeline_dag.py   # Full pipeline DAG (daily)
│       └── dbt_dag.py                   # dbt-only rerun DAG
├── api/
│   ├── main.py                          # FastAPI app entry point
│   ├── agents/text_to_sql.py            # LangChain + Ollama agent
│   ├── models/
│   │   ├── db.py                        # Async DB connection pool
│   │   └── schemas.py                   # Pydantic response models
│   └── routes/
│       ├── analytics.py                 # Readmission, cost, patient endpoints
│       ├── query.py                     # Text-to-SQL endpoint
│       └── health.py                    # Liveness / readiness probes
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/                     # stg_* views (clean raw data)
│       ├── intermediate/                # int_* ephemeral models (features)
│       └── marts/                       # mart_* tables (analytics layer)
│           ├── mart_readmission_analysis.sql
│           ├── mart_cost_analysis.sql
│           └── mart_patient_summary.sql
├── data/seed_data/                      # Generated synthetic CSVs
├── ingestion/load_data.py               # CSV → PostgreSQL raw loader
├── scripts/
│   ├── generate_data.py                 # Synthetic data generator (65k rows)
│   └── init_db.sql                      # PostgreSQL schema bootstrap
├── docker-compose.yml
├── Dockerfile.api
├── requirements.txt
└── .env.example
```

---

## Quick Start

### Prerequisites
- Docker Desktop ≥ 4.x
- 8 GB RAM recommended (Ollama + Postgres + Airflow)

### 1. Clone & configure

```bash
git clone https://github.com/jramkrishnan/healthcare-analytics-pipeline.git
cd healthcare-analytics-pipeline
cp .env.example .env
```

### 2. Start the full stack

```bash
docker compose up -d
```

This will:
- Start PostgreSQL and apply `init_db.sql`
- Start Ollama and pull `llama3` (~4 GB download on first run)
- Initialise Airflow and create an admin user
- Start the FastAPI service

### 3. Trigger the pipeline

```bash
# Option A — via Airflow UI (recommended)
open http://localhost:8080
# Login: admin / admin
# Trigger: healthcare_analytics_pipeline

# Option B — CLI
docker exec healthcare_airflow_scheduler \
  airflow dags trigger healthcare_analytics_pipeline
```

The DAG will:
1. Validate or generate seed CSV files
2. Load ~65k rows into `raw.*` PostgreSQL tables
3. Run dbt staging → intermediate → mart models
4. Execute dbt tests
5. Generate dbt docs

### 4. Explore the API

```bash
open http://localhost:8000/docs
```

Key endpoints:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/analytics/readmission` | Hospital readmission rates |
| GET | `/analytics/readmission/summary` | National summary stats |
| GET | `/analytics/cost` | Medicare cost by hospital + DRG |
| GET | `/analytics/cost/drg-benchmark` | National DRG benchmarks |
| GET | `/analytics/patients` | Patient risk stratification |
| GET | `/analytics/patients/demographics` | Demographic breakdown |
| POST | `/query` | Natural language → SQL |
| GET | `/query/examples` | Sample questions |

### 5. Try Text-to-SQL

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"question": "Which hospitals in MA had a readmission rate above 20% in 2022?"}'
```

All inference runs on `ollama` container — no data sent to OpenAI or any external API.

---

## Data Model

### Raw Schema (`raw.*`)
Direct mirror of incoming CSV data. No transformations applied.

```
raw.hospitals   →  raw.admissions  ←─  raw.patients
                         │
                   raw.diagnoses
raw.medicare_costs
```

### dbt Lineage
```
raw.* (sources)
  └── stg_hospitals / stg_patients / stg_admissions / stg_diagnoses / stg_medicare_costs
        └── int_patient_admissions
              └── int_readmission_features
                    ├── mart_readmission_analysis   (analytics.*)
                    ├── mart_cost_analysis
                    └── mart_patient_summary
```

### Key Metrics

**Readmission Analysis**
- 30-day readmission rate per hospital (CMS standard metric)
- Risk score per admission: weighted composite of LOS, comorbidities, age, ICU hours
- Benchmarked vs national average

**Cost Analysis**
- Medicare payment ratio: `avg_medicare_payments / avg_covered_charges`
- Cost efficiency flag: High/Low outlier if ±25% from national DRG average
- Estimated write-off exposure per hospital

**Patient Risk Stratification**
- Risk tiers: Low / Medium / High
- High-utiliser flag: ≥5 admissions or >$100k lifetime charges
- Comorbidity burden from secondary ICD-10 codes

---

## dbt Models

Run dbt independently:

```bash
cd dbt
dbt deps
dbt run --select staging
dbt run --select intermediate
dbt run --select marts
dbt test
dbt docs serve   # opens docs at http://localhost:8080
```

---

## Design Decisions

**Why local Ollama instead of OpenAI?**
Healthcare data is sensitive. Running inference on-premise ensures no patient-related data is transmitted to third-party APIs, which is a prerequisite for HIPAA-aligned environments.

**Why dbt for transformations?**
dbt brings software engineering best practices (version control, testing, documentation) to SQL transformations. The staging → intermediate → mart layering keeps models modular and independently testable.

**Why asyncpg + databases for the API?**
FastAPI's async nature pairs naturally with an async PostgreSQL driver, giving high throughput for concurrent dashboard queries without blocking.

**Synthetic data**
The 65k-row dataset is generated deterministically (seed=42) so results are reproducible across environments.

---

## Acknowledgements

Synthetic data modelled after CMS Medicare Provider Utilization datasets and MIMIC-III admission patterns. No real patient data is used in this project.
