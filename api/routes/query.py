"""
api/routes/query.py
Natural language → SQL endpoint backed by the LangChain + Ollama agent.
"""

import logging
from fastapi import APIRouter, HTTPException

from api.models.schemas import QueryRequest, QueryResponse
from api.agents.text_to_sql import get_agent

log    = logging.getLogger(__name__)
router = APIRouter()


@router.post(
    "/",
    response_model = QueryResponse,
    summary        = "Natural language query (Text-to-SQL)",
    description    = (
        "Ask a question in plain English about hospital readmissions, costs, or patients. "
        "The request is handled entirely on-premise by a local Ollama LLM — "
        "no data is sent to external APIs."
    ),
)
async def natural_language_query(body: QueryRequest):
    try:
        agent  = get_agent()
        result = agent.run(question=body.question, max_rows=body.max_rows)
        return QueryResponse(**result)

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        log.error(f"Text-to-SQL error: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=(
                "Query execution failed. Ensure the Ollama service is running "
                f"and the model is available. Detail: {exc}"
            ),
        )


@router.get(
    "/examples",
    summary="Sample questions you can ask the Text-to-SQL endpoint",
)
async def example_questions():
    return {
        "examples": [
            "Which hospitals in MA had a readmission rate above 20% in 2022?",
            "What are the top 5 DRG codes by total Medicare write-offs?",
            "Show me the average charges for heart failure admissions by hospital size.",
            "How many high-risk patients were discharged to SNF vs home?",
            "Compare readmission rates between teaching and non-teaching hospitals.",
            "Which hospitals are high cost outliers for DRG 470?",
            "What percentage of Medicare-eligible patients had ICU stays?",
            "Show monthly readmission trends for 2022.",
        ]
    }
