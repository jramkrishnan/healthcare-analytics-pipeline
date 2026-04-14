# Healthcare Analytics Pipeline

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
│                                          │  (qwen2:1.5b)│  │
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
| LLM Inference | Ollama (qwen2:1.5b — local) |
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
│   ├── macros/
│   │   └── generate_schema_name.sql     # Ensures marts land in analytics.* not analytics_analytics.*
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
│   ├── 00_create_airflow_db.sh          # Creates Airflow user and database
│   └── 01_init_healthcare.sql           # Healthcare schema, tables, and indexes
├── docker-compose.yml
├── Dockerfile.api
├── requirements.txt                     # API dependencies (SQLAlchemy 2.x)
├── requirements-airflow.txt             # Airflow DAG dependencies (no SQLAlchemy)
└── .env.example
```

---

## Quick Start

### Prerequisites
- Docker Desktop >= 4.x with at least **6 GB RAM** allocated (Settings → Resources → Memory)
- Ollama pulls `qwen2:1.5b` (~935 MB) on first start — ensure enough disk space

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
- Start PostgreSQL and run `00_create_airflow_db.sh` (creates the Airflow user/database) then `01_init_healthcare.sql` (healthcare schema and raw tables)
- Start Ollama and pull `qwen2:1.5b` (~935 MB download on first run)
- Initialise Airflow and create an admin user
- Start the FastAPI service

### 3. Generate and load data

The seed data must be generated and loaded before triggering the DAG or querying the API.

```bash
# Generate 65k rows of synthetic CSV data
docker exec healthcare_airflow_scheduler python /opt/airflow/scripts/generate_data.py

# Load CSVs into raw.* PostgreSQL tables
docker exec healthcare_airflow_scheduler python /opt/airflow/ingestion/load_data.py
```

### 4. Build the dbt analytics tables

```bash
docker exec healthcare_airflow_scheduler \
  /home/airflow/.local/bin/dbt run \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt \
  --full-refresh
```

This builds three mart tables in the `analytics` schema:
- `analytics.mart_readmission_analysis` — 607 rows
- `analytics.mart_cost_analysis` — 708 rows
- `analytics.mart_patient_summary` — 9,470 rows

Verify:

```bash
docker exec healthcare_postgres psql -U postgres -d healthcare -c "\dt analytics.*"
```

### 5. Explore the API

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

### 6. Try Text-to-SQL

```bash
curl -X POST http://localhost:8000/query/ \
  -H "Content-Type: application/json" \
  -d '{"question": "Which hospitals had the highest readmission rates?", "max_rows": 5}'
```

All inference runs on the `ollama` container — no data is sent to OpenAI or any external API.

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

Run dbt independently inside the Airflow scheduler container:

```bash
docker exec healthcare_airflow_scheduler \
  /home/airflow/.local/bin/dbt run \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt

docker exec healthcare_airflow_scheduler \
  /home/airflow/.local/bin/dbt test \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt
```

---

## Design Decisions

**Why local Ollama instead of OpenAI?**
Healthcare data is sensitive. Running inference on-premise ensures no patient-related data is transmitted to third-party APIs, which is a prerequisite for HIPAA-aligned environments.

**Why qwen2:1.5b?**
It is the smallest model that produces reliably correct SQL for this schema. Larger models (phi3:mini at 3.8B) work better but require more memory. The model choice is controlled by `OLLAMA_MODEL` in `.env` — swap it without rebuilding any container.

**Why split requirements.txt?**
Airflow 2.9 bundles SQLAlchemy < 2.0 internally. The FastAPI service requires SQLAlchemy 2.x. Putting both in the same `requirements.txt` breaks Airflow's ORM. The split — `requirements.txt` for the API image and `requirements-airflow.txt` for the Airflow containers — keeps each environment clean.

**Why the generate_schema_name macro?**
dbt's default behaviour concatenates the target schema from `profiles.yml` with the custom `+schema` defined in `dbt_project.yml`, producing names like `analytics_analytics`. The macro in `dbt/macros/generate_schema_name.sql` overrides this so marts land directly in `analytics.*` as expected.

**Why dbt for transformations?**
dbt brings software engineering best practices (version control, testing, documentation) to SQL transformations. The staging → intermediate → mart layering keeps models modular and independently testable.

**Why asyncpg + databases for the API?**
FastAPI's async nature pairs naturally with an async PostgreSQL driver, giving high throughput for concurrent dashboard queries without blocking.

**Synthetic data**
The 65k-row dataset is generated deterministically (seed=42) so results are reproducible across environments.

---

## Acknowledgements

Synthetic data modelled after CMS Medicare Provider Utilization datasets and MIMIC-III admission patterns. No real patient data is used in this project.
