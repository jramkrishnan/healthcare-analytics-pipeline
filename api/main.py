"""
Healthcare Analytics API
FastAPI application exposing readmission, cost, and patient analytics
from dbt-transformed PostgreSQL tables, plus a Text-to-SQL query endpoint
powered by a local Ollama LLM.
"""

from contextlib import asynccontextmanager
import logging
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.routes import analytics, query, health
from api.models.db import database

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Connect to DB on startup, disconnect on shutdown."""
    log.info("Connecting to database …")
    await database.connect()
    yield
    log.info("Disconnecting from database …")
    await database.disconnect()


app = FastAPI(
    title       = "Healthcare Analytics API",
    description = (
        "End-to-end healthcare data pipeline API.\n\n"
        "Surfaces readmission rates, Medicare cost benchmarks, and patient "
        "risk stratification from dbt-transformed PostgreSQL analytics tables.\n\n"
        "Includes a `/query` endpoint backed by a LangChain Text-to-SQL agent "
        "running on a locally-hosted Ollama LLM — no PHI leaves the server."
    ),
    version     = "1.0.0",
    lifespan    = lifespan,
)

# ── CORS (allow local dev front-ends) ─────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["http://localhost:3000", "http://localhost:8501"],
    allow_methods  = ["*"],
    allow_headers  = ["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(health.router,    prefix="/health",    tags=["Health"])
app.include_router(analytics.router, prefix="/analytics", tags=["Analytics"])
app.include_router(query.router,     prefix="/query",     tags=["Text-to-SQL"])


@app.get("/", include_in_schema=False)
async def root():
    return JSONResponse({
        "name":    "Healthcare Analytics API",
        "version": "1.0.0",
        "docs":    "/docs",
    })
