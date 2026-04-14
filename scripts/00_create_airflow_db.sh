#!/bin/bash
# =============================================================================
# 00_create_airflow_db.sh
# Creates the Airflow PostgreSQL user and database.
# This MUST be a .sh file (not .sql) because CREATE DATABASE cannot run
# inside a transaction block — shell scripts in docker-entrypoint-initdb.d
# are executed with psql outside of any transaction.
# Runs before 01_init_healthcare.sql due to filename ordering.
# =============================================================================

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Airflow needs its own user and database separate from healthcare
    CREATE USER airflow WITH PASSWORD 'airflow';
    CREATE DATABASE airflow OWNER airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
EOSQL

echo "✅  Airflow database and user created."
