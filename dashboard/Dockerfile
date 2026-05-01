FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY dashboard/requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

EXPOSE 8501

ENV PYTHONPATH=/app

CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
