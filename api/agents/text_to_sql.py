"""
api/agents/text_to_sql.py
LangChain Text-to-SQL agent backed by a locally-hosted Ollama LLM.
No data leaves the server — important for HIPAA-aligned environments.

Flow:
  1. User natural-language question
  2. LangChain agent inspects analytics schema via SQLDatabase
  3. Ollama (phi3:mini) generates SQL
  4. SQL executed against PostgreSQL
  5. Results + generated SQL returned to caller
"""

import logging
import os
import re

from langchain_community.utilities import SQLDatabase
from langchain_community.llms import Ollama
from langchain.chains import create_sql_query_chain
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from sqlalchemy import text

from api.models.db import sync_engine

log = logging.getLogger(__name__)

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL",    "qwen2:0.5b")

# Tables the agent is allowed to query (dbt marts only — no raw PHI tables)
ALLOWED_TABLES = [
    "mart_readmission_analysis",
    "mart_cost_analysis",
    "mart_patient_summary",
]

# ── Prompt ────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Write a single PostgreSQL SELECT for this question.
Schema:
  analytics.mart_readmission_analysis(hospital_id,hospital_name,hospital_state,hospital_type,hospital_size,teaching_flag,admit_year,admit_quarter,total_admissions,readmissions,readmission_rate_pct,avg_los_days,avg_charges,avg_risk_score,high_risk_patients,medium_risk_patients,low_risk_patients,avg_comorbidities,icu_admissions,avg_icu_hours,national_avg_readmission_rate,vs_national_avg)
  analytics.mart_cost_analysis(hospital_id,hospital_name,state,hospital_type,hospital_size,teaching_flag,drg_code,drg_description,year,total_discharges,avg_covered_charges,avg_total_payments,avg_medicare_payments,medicare_payment_ratio,cost_payment_gap,volume_tier,national_avg_charges,national_avg_payments,national_avg_medicare,charge_vs_national,charge_pct_vs_national,cost_efficiency_flag,write_off_per_discharge,total_write_off_estimate)
  analytics.mart_patient_summary(patient_id,age,age_group,gender,race,insurance_type,medicare_eligible,patient_state,total_admissions,first_admission_date,last_admission_date,days_in_system,total_readmissions,personal_readmission_rate_pct,avg_los,total_inpatient_days,avg_charge_per_visit,total_lifetime_charges,icu_visits,total_icu_hours,avg_comorbidity_count,max_comorbidity_count,avg_risk_score,peak_risk_score,most_common_diagnosis,high_utiliser)
Rules: one SELECT only, no markdown, LIMIT {max_rows}, only columns listed above.
Question: {question}
SQL:"""

PROMPT = PromptTemplate(
    input_variables=["question", "max_rows"],
    template=SYSTEM_PROMPT,
)


# ── Agent ─────────────────────────────────────────────────────────────────────

class TextToSQLAgent:
    """Wraps an Ollama LLM and executes LangChain-generated SQL safely."""

    def __init__(self):
        self.llm = Ollama(
            base_url    = OLLAMA_BASE_URL,
            model       = OLLAMA_MODEL,
            temperature = 0.0,       # deterministic SQL generation
        )
        self.chain = PROMPT | self.llm | StrOutputParser()

    def _sanitize_sql(self, raw_sql: str) -> str:
        """Strip markdown fences, extract first SELECT, validate safety."""
        sql = raw_sql.strip()
        # Remove ```sql ... ``` fences
        sql = re.sub(r"```(?:sql)?", "", sql, flags=re.IGNORECASE).strip().rstrip("```").strip()
        # Extract just the first SELECT statement (models sometimes emit two)
        first_select = re.split(r"(?i)\bSELECT\b", sql, maxsplit=1)
        if len(first_select) < 2:
            raise ValueError(f"Only SELECT queries are permitted. Got: {sql[:80]}")
        # Find the end of the first statement (semicolon or a second SELECT keyword)
        body = first_select[1]
        second_select = re.search(r"(?i);\s*SELECT\b|(?<=\n)SELECT\b", body)
        if second_select:
            body = body[: second_select.start()]
        sql = ("SELECT " + body).rstrip("; \n")
        # Remove any embedded semicolons the model placed before LIMIT/ORDER BY
        sql = re.sub(r";\s*(LIMIT|ORDER\s+BY|GROUP\s+BY|HAVING)", r" \1", sql, flags=re.IGNORECASE)
        # Only SELECT statements allowed
        if not sql.upper().startswith("SELECT"):
            raise ValueError(f"Only SELECT queries are permitted. Got: {sql[:80]}")
        # Block access to raw/staging schemas
        if re.search(r"\braw\.|staging\.", sql, re.IGNORECASE):
            raise ValueError("Direct access to raw or staging schemas is not allowed.")
        # Ensure all known mart tables are schema-qualified (small models often omit the prefix)
        for table in ALLOWED_TABLES:
            sql = re.sub(
                rf"(?<!analytics\.)\b{re.escape(table)}\b",
                f"analytics.{table}",
                sql,
                flags=re.IGNORECASE,
            )
        return sql

    def run(self, question: str, max_rows: int = 50) -> dict:
        log.info(f"Text-to-SQL question: {question!r}")

        # 1. Generate SQL
        raw_sql = self.chain.invoke({"question": question, "max_rows": max_rows})
        sql     = self._sanitize_sql(raw_sql)
        log.info(f"Generated SQL:\n{sql}")

        # 2. Execute
        with sync_engine.connect() as conn:
            result = conn.execute(text(sql))
            cols   = list(result.keys())
            rows   = [dict(zip(cols, row)) for row in result.fetchmany(max_rows)]

        return {
            "question":   question,
            "sql":        sql,
            "results":    rows,
            "row_count":  len(rows),
            "model_used": f"ollama/{OLLAMA_MODEL}",
        }


# Singleton — avoids reinitialising the LLM on every request
_agent: TextToSQLAgent | None = None


def get_agent() -> TextToSQLAgent:
    global _agent
    if _agent is None:
        _agent = TextToSQLAgent()
    return _agent
