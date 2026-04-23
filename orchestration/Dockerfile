FROM python:3.12-slim

WORKDIR /app

# install pg_config so psycopg2 can build from source
RUN apt-get update && apt-get install -y libpq-dev gcc python3-dev && rm -rf /var/lib/apt/lists/*

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY orchestration/requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

COPY orchestration/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 CMD prefect worker ls || exit 1

CMD ["/entrypoint.sh"]
