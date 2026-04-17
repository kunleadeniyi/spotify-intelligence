#!/bin/bash
set -e

echo "Creating users and databases..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL

-- User 1 + DB 1
CREATE USER "${PG_SPOTIFY_DB_USER}" WITH PASSWORD '${PG_SPOTIFY_DB_USER_PASSWORD}';
CREATE DATABASE ${PG_SPOTIFY_DB} OWNER "${PG_SPOTIFY_DB_USER}";
GRANT ALL PRIVILEGES ON DATABASE ${PG_SPOTIFY_DB} TO "${PG_SPOTIFY_DB_USER}";

-- User 2 + DB 2
CREATE USER "${PG_PREFECT_USER}" WITH PASSWORD '${PG_PREFECT_USER_PASSWORD}';
CREATE DATABASE ${PG_PREFECT_DB} OWNER "${PG_PREFECT_USER}";
GRANT ALL PRIVILEGES ON DATABASE ${PG_PREFECT_DB} TO "${PG_PREFECT_USER}";

EOSQL

echo "Creating schemas and tables in ${PG_SPOTIFY_DB}..."

psql -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$PG_SPOTIFY_DB" \
    --file /docker-entrypoint-initdb.d/schema.sql

echo "Done."