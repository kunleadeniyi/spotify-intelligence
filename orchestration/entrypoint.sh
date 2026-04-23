#!/bin/bash
set -e

echo "Registering dbt deployment..."
python /app/orchestration/flows/dbt_flow.py

echo "Registering feast materialization deployment..."
python /app/orchestration/flows/feature_flow.py

echo "Starting Prefect worker..."
exec prefect worker start --pool local-pool
