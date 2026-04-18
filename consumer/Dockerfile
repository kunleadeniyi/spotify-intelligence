FROM python:3.12-slim

WORKDIR /app

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY consumer/requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

# context will be set in docker compose file to be the project root directory because of requirements.txt
COPY consumer/ ./consumer/

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD pgrep -f "python consumer/consumer.py" || exit 1

CMD [ "python", "-m", "consumer.consumer" ]