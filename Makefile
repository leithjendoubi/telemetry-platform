# Telemetry platform: developer entry points (spec FR-PLT-01, 14.2).
# Stage 2: infrastructure + ingestion gateway + detection stub + analytics service, and the end-to-end smoke test.
# (Leith works on Windows without make: the README gives the same commands for PowerShell.)

COMPOSE      ?= docker compose -f infra/compose/docker-compose.yml --profile core
SIM_DIR      := simulator
SIM_PY       ?= python3
SIM_ENV      := PYTHONPATH=src
SIM_CONFIG   ?= configs/default.yaml
SIM_FORMAT   ?= parquet

.DEFAULT_GOAL := help

.PHONY: help up up-infra down clean logs ps topics psql-analytics smoke \
        sim-install sim-test sim-describe sim-data sim-live

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

# ---------------------------------------------------------------- infrastructure
.env:
	cp .env.example .env

up: .env  ## Build and start everything (infrastructure + the three services)
	$(COMPOSE) --env-file .env up -d --build --wait
	@echo "Gateway: http://localhost:8081   Analytics: http://localhost:8082   Kafka: localhost:9092   PostgreSQL: localhost:5432"

up-infra: .env  ## Start only Kafka + TimescaleDB, create topics and databases
	$(COMPOSE) --env-file .env up -d --wait kafka timescaledb
	$(COMPOSE) --env-file .env up kafka-init

smoke: .env  ## End-to-end smoke test from a clean start (DELETES the stack's data first)
	$(SIM_PY) bench/scripts/smoke_test.py --clean

down:  ## Stop the stack (data is kept)
	$(COMPOSE) down

clean:  ## Stop the stack and DELETE all data volumes
	$(COMPOSE) down -v

logs:  ## Follow logs of all containers
	$(COMPOSE) logs -f

ps:  ## Show container status
	$(COMPOSE) ps

topics:  ## List Kafka topics
	docker exec telemetry-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list

psql-analytics:  ## Open a psql shell in analytics_db
	docker exec -it telemetry-timescaledb psql -U postgres -d analytics_db

# ---------------------------------------------------------------- simulator
sim-install:  ## Install the simulator and its dependencies (pip, editable)
	cd $(SIM_DIR) && $(SIM_PY) -m pip install -e ".[dev]"

sim-test:  ## Run the simulator tests (unittest; pytest also works)
	cd $(SIM_DIR) && $(SIM_ENV) $(SIM_PY) -m unittest discover -s tests -v

sim-describe:  ## Print the fleet model (instances, events per day, rate)
	cd $(SIM_DIR) && $(SIM_ENV) $(SIM_PY) -m fleet_sim describe --config $(SIM_CONFIG)

sim-data:  ## Generate 1 day of data (use DAYS=10 for dataset D1)
	cd $(SIM_DIR) && $(SIM_ENV) $(SIM_PY) -m fleet_sim generate --config $(SIM_CONFIG) \
	  --out data/$${NAME:-sample} --start $${START:-2026-01-01} --days $${DAYS:-1} --format $(SIM_FORMAT)

sim-live:  ## Send live traffic to the gateway (needs the gateway from stage 2)
	cd $(SIM_DIR) && $(SIM_ENV) $(SIM_PY) -m fleet_sim live --config $(SIM_CONFIG) \
	  --url $${URL:-http://localhost:8081/api/v1/telemetry/ingest} --speed $${SPEED:-1} $${DURATION:+--duration $$DURATION}
