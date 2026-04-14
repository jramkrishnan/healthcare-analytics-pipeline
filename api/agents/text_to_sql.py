"""
api/agents/text_to_sql.py
LangChain Text-to-SQL agent backed by a locally-hosted Ollama LLM.
No data leaves the server — important for HIPAA-aligned environments.

Flow:
  1. User natural-language question
  2. LangChain agent inspects analytics schema via SQLDatabase
  3. Ollama (llama3) generates SQL
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
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL",    "llama3")

# Tables the agent is allowed to query (dbt marts only — no raw PHI tables)
ALLOWED_TABLES = [
    "mart_readmission_analysis",
    "mart_cost_analysis",
    "mart_patient_summary",
]

# ── Prompt ────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a healthcare data analyst assistant. 
You have access to a PostgreSQL database with the following analytics tables in the 'analytics' schema:

- mart_readmission_analysis: Hospital 30-day readmission rates by year/quarter, including risk scores and charges.
- mart_cost_analysis: Medicare cost data per hospital and DRG code with national benchmarks.
- mart_patient_summary: One row per patient with lifetime clinical metrics and risk stratification.

Rules:
1. ONLY query the analytics schema. Never query raw or staging schemas.
2. Always use fully qualified table names: analytics.<table_name>
3. Limit results to {max_rows} rows unless the user asks for aggregates.
4. Return ONLY the SQL query, nothing else. No explanation, no markdown fences.
5. Use lowercase column and table names.
6. For readmission rates, the column is readmission_rate_pct (already as percentage).

Question: {question}

SQL Query:"""

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
        """Strip markdown fences and trailing semicolons; validate safety."""
        sql = raw_sql.strip()
        # Remove ```sql ... ``` fences if the model adds them anyway
        sql = re.sub(r"```(?:sql)?", "", sql, flags=re.IGNORECASE).strip().rstrip("```").strip()
        # Only SELECT statements allowed
        if not sql.upper().startswith("SELECT"):
            raise ValueError(f"Only SELECT queries are permitted. Got: {sql[:80]}")
        # Block access to raw/staging schemas
        if re.search(r"\braw\.|staging\.", sql, re.IGNORECASE):
            raise ValueError("Direct access to raw or staging schemas is not allowed.")
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
