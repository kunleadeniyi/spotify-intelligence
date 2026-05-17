.PHONY: up down reset logs test lint dbt-run dbt-test kafka-topics install dashboard observability cdc-up cdc-down cdc-register cdc-status cdc-reset help

# ─────────────────────────────────────────────
# Environment
# ─────────────────────────────────────────────

install:
	uv pip install -e ".[consumer,producer,api,dashboard,orchestration,dev]"

# ─────────────────────────────────────────────
# Infrastructure
# ─────────────────────────────────────────────

up:
	docker compose up -d
	@echo "All services started. Run 'make logs' to follow output."

down:
	docker compose down

reset:
	@echo "WARNING: This will destroy all volumes and data."
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	docker compose down -v --remove-orphans
	rm -rf ./volumes/postgres/data/
	@echo "All containers and volumes removed."

logs:
	docker compose logs -f

logs-%:
	docker compose logs -f $*

kafka-topics:
	docker exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# ─────────────────────────────────────────────
# Testing
# ─────────────────────────────────────────────

test:
	pytest tests/ -v

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

# ─────────────────────────────────────────────
# Linting
# ─────────────────────────────────────────────

lint:
	ruff check .
	ruff format --check .

lint-fix:
	ruff check --fix .
	ruff format .

# ─────────────────────────────────────────────
# Dashboard
# ─────────────────────────────────────────────

dashboard:
	PYTHONPATH=. streamlit run dashboard/app.py

# ─────────────────────────────────────────────
# Observability
# ─────────────────────────────────────────────

observability:
	docker compose --profile observability up -d
	@echo "Grafana: http://localhost:3000 (admin / admin)"
	@echo "Prometheus: http://localhost:9090"

# ─────────────────────────────────────────────
# dbt
# ─────────────────────────────────────────────

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve

# ─────────────────────────────────────────────
# CDC (Kafka Connect + Debezium + MinIO)
# ─────────────────────────────────────────────

cdc-up:
	docker compose --profile cdc up -d
	@echo "CDC services started."
	@echo "  Kafka Connect: http://localhost:8083"
	@echo "  Schema Registry: http://localhost:8081"
	@echo "  MinIO console:   http://localhost:9001"

cdc-down:
	docker compose --profile cdc down

cdc-register:
	@echo "Registering Debezium source connector..."
	@set -a && . ./.env && set +a && \
		envsubst < debezium/connectors/source.json | \
		python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps(d['config']))" | \
		curl -sf -X PUT http://localhost:8083/connectors/postgres-cdc-source/config \
		-H "Content-Type: application/json" \
		-d @- | python3 -m json.tool
	@echo "Registering S3 sink connector..."
	@set -a && . ./.env && set +a && \
		envsubst < debezium/connectors/sink.json | \
		python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps(d['config']))" | \
		curl -sf -X PUT http://localhost:8083/connectors/s3-cdc-sink/config \
		-H "Content-Type: application/json" \
		-d @- | python3 -m json.tool

cdc-status:
	@echo "=== Connectors ==="
	@curl -s http://localhost:8083/connectors?expand=status | python3 -m json.tool
	@echo "=== Replication slot lag ==="
	@docker exec si-postgres psql -U "$${POSTGRES_ADMIN_USER}" -d "$${PG_SPOTIFY_DB}" \
		-c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots;"

cdc-reset:
	@echo "Deleting connectors..."
	-curl -sf -X DELETE http://localhost:8083/connectors/postgres-cdc-source
	-curl -sf -X DELETE http://localhost:8083/connectors/s3-cdc-sink
	@echo "Dropping replication slot..."
	-docker exec si-postgres psql -U "$${POSTGRES_ADMIN_USER}" -d "$${PG_SPOTIFY_DB}" \
		-c "SELECT pg_drop_replication_slot('debezium_slot') FROM pg_replication_slots WHERE slot_name = 'debezium_slot';"
	@echo "CDC reset complete. Run 'make cdc-register' to re-register connectors."

# ─────────────────────────────────────────────
# Help
# ─────────────────────────────────────────────

help:
	@echo ""
	@echo "Spotify Intelligence Platform"
	@echo "─────────────────────────────"
	@echo "  make install         Install Python dependencies via uv"
	@echo "  make up              Start all Docker services"
	@echo "  make down            Stop all Docker services"
	@echo "  make reset           Destroy all containers and volumes (destructive)"
	@echo "  make logs            Tail logs for all services"
	@echo "  make logs-<service>  Tail logs for a specific service (e.g. make logs-kafka)"
	@echo "  make kafka-topics    List all Kafka topics"
	@echo "  make test            Run full test suite"
	@echo "  make test-unit       Run unit tests only"
	@echo "  make test-integration  Run integration tests only"
	@echo "  make lint            Check code style (ruff)"
	@echo "  make lint-fix        Auto-fix code style issues"
	@echo "  make dbt-run         Run all dbt models"
	@echo "  make dbt-test        Run dbt data tests"
	@echo "  make dbt-docs        Generate and serve dbt docs"
	@echo "  make dashboard       Run Streamlit dashboard locally"
	@echo "  make observability   Start observability stack (Grafana, Prometheus, Loki, Alloy)"
	@echo "  make cdc-up          Start CDC profile services (Debezium, Kafka Connect, MinIO, Schema Registry)"
	@echo "  make cdc-down        Stop CDC profile services"
	@echo "  make cdc-register    POST connector configs to Kafka Connect REST API"
	@echo "  make cdc-status      Show connector status and replication slot lag"
	@echo "  make cdc-reset       Delete connectors and drop replication slot"
	@echo ""
