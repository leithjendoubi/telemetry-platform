#!/usr/bin/env bash
# Runs once, on the first start of the TimescaleDB container (empty data volume).
# Creates the three LOGICAL databases of spec 6.3, each with its own role and password,
# so no service can read another service's tables ("database per service", principle 3).
#   ingestion_db  (agent registry)           owner: INGESTION_DB_USER
#   analytics_db  (TimescaleDB hypertable)   owner: ANALYTICS_DB_USER
#   alerting_db   (alerts, outbox)           owner: ALERTING_DB_USER
set -euo pipefail

create_db() {
  local db="$1" user="$2" password="$3"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-SQL
	CREATE ROLE ${user} LOGIN PASSWORD '${password}';
	CREATE DATABASE ${db} OWNER ${user};
	REVOKE ALL ON DATABASE ${db} FROM PUBLIC;
	GRANT CONNECT ON DATABASE ${db} TO ${user};
SQL
  echo "created database ${db} (owner ${user})"
}

create_db ingestion_db "$INGESTION_DB_USER" "$INGESTION_DB_PASSWORD"
create_db analytics_db "$ANALYTICS_DB_USER" "$ANALYTICS_DB_PASSWORD"
create_db alerting_db  "$ALERTING_DB_USER"  "$ALERTING_DB_PASSWORD"

# The TimescaleDB extension is needed only in analytics_db. Creating an extension needs superuser,
# so it is done here; the analytics service's Flyway migrations (stage 2) then create the tables.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname analytics_db \
  -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
echo "timescaledb extension enabled in analytics_db"
