"""api/routes/health.py — liveness & readiness endpoints."""

from fastapi import APIRouter
from api.models.db import database

router = APIRouter()


@router.get("/", summary="Liveness check")
async def liveness():
    return {"status": "ok"}


@router.get("/ready", summary="Readiness check — verifies DB connectivity")
async def readiness():
    try:
        await database.execute("SELECT 1")
        return {"status": "ready", "database": "connected"}
    except Exception as exc:
        return {"status": "not ready", "database": str(exc)}
