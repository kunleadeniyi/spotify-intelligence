#!/bin/bash
set -e

: "${MLFLOW_ADMIN_USER:?MLFLOW_ADMIN_USER is not set}"
: "${MLFLOW_ADMIN_PASSWORD:?MLFLOW_ADMIN_PASSWORD is not set}"
: "${MLFLOW_BACKEND_STORE_URI:?MLFLOW_BACKEND_STORE_URI is not set}"

pip install uv --quiet
uv pip install psycopg2-binary --system --quiet

cat > /mlflow/auth.ini << EOF
[mlflow]
default_permission = READ
database_uri = sqlite:////mlflow/basic_auth.db
admin_username = ${MLFLOW_ADMIN_USER}
admin_password = ${MLFLOW_ADMIN_PASSWORD}
EOF

exec mlflow server \
  --backend-store-uri "${MLFLOW_BACKEND_STORE_URI}" \
  --artifacts-destination /mlflow/artifacts \
  --app-name basic-auth \
  --host 0.0.0.0 \
  --port 5555
