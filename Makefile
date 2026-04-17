.PHONY: up down reset logs test lint dbt-run dbt-test kafka-topics help

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
# dbt
# ─────────────────────────────────────────────

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve

# ─────────────────────────────────────────────
# Help
# ─────────────────────────────────────────────

help:
	@echo ""
	@echo "Spotify Intelligence Platform"
	@echo "─────────────────────────────"
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
	@echo ""
