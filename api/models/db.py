"""
api/models/db.py
Async database connection via `databases` library (backed by asyncpg).
All route handlers share this single connection pool.
"""

import os
import databases
import sqlalchemy

DATABASE_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}"
    f":{os.getenv('POSTGRES_PASSWORD', 'postgres')}"
    f"@{os.getenv('POSTGRES_HOST', 'localhost')}"
    f":{os.getenv('POSTGRES_PORT', '5432')}"
    f"/{os.getenv('POSTGRES_DB', 'healthcare')}"
)

database = databases.Database(DATABASE_URL)

# Sync engine used only for schema inspection (LangChain agent)
sync_engine = sqlalchemy.create_engine(DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://"))
