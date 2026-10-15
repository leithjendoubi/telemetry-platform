# CLAUDE.md: project briefing

Read this first in every session.

## The project
Final-year engineering project (PFE) of Leith: an event-driven telemetry ingestion and real-time
anomaly-detection platform built as microservices. The authoritative requirements are the Engineering
Project Specification CAP-PFE-2026-TEL-084 Rev 2.0: **the PDF in this folder whose name contains
`CAP-PFE-2026-TEL-084`**. When this file and the spec disagree, the spec wins; tell Leith about the
conflict instead of choosing silently. Section numbers like "spec 8.1" refer to that PDF.

Key spec sections: 6.5 technology stack, 8.1 ingest contract, 8.2 query API, 8.3 Kafka topics,
8.4 database schema, 7 functional requirements (FR-ING, FR-DET, FR-ANA, ...), 16.2 week-by-week plan.

## How to work with Leith
- Leith is learning while building. After each piece of work, explain in simple words what was built,
  why, and how to run it. Avoid unexplained jargon.
- Work in small steps that each end in something runnable and verified. Run the build and tests
  yourself and show the result; never claim something works without having run it.
- He works on **Windows with PowerShell**. Give PowerShell commands, not bash. `make` is not
  available; use the raw `docker compose` commands below.
- Ask before big or irreversible decisions (new technology, changing the contract, deleting data).

## Machine setup (verified 2026-10-14)
- Windows, PowerShell, Docker Desktop (must show "Engine running" before docker commands).
- Python 3.12 in `simulator\.venv`. The system `python` is 3.10 and too old: always use
  `.venv\Scripts\python`, or `py -3.12` to create new environments.
- Java: the default `java` is **17**. JDK 21 (Temurin 21.0.12, installed 2026-10-14 with winget) is at
  `C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot`. Set
  `$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot"` before `mvn`. Maven 3.9.5.
- **Port 8080 is taken by Leith's Pi-hole container** (also port 53). Do not stop it. The gateway's
  default port is therefore **8081** everywhere (config, simulator default `--url`, compose).
- Docker Engine 29: Testcontainers must be 1.21.4 or newer (1.21.3 fails with HTTP 400).
- In Git Bash, `docker exec ... /opt/kafka/...` needs `MSYS_NO_PATHCONV=1` (PowerShell is fine).

## Current status
**Stage 1 (spec weeks 1-3): done and verified on Leith's Windows machine.**
- `simulator/`: fleet simulator v0 (normal traffic only). 53 tests pass (1 skip is expected: the "pyarrow missing" path), Parquet included.
  Same seed gives bit-identical data on Linux and Windows (fingerprint check done).
- `infra/compose/`: Kafka (KRaft, single broker) + TimescaleDB with 3 databases
  (`ingestion_db`, `analytics_db`, `alerting_db`, one owner role each, PUBLIC access revoked).
  Containers start healthy, 10 topics created. The "period or underscore" WARNING from
  kafka-topics is harmless (names use dots only).
- `libs/telemetry-contracts/`: JSON Schema v1.0 of the ingest payload + valid example +
  16 invalid cases (`invalid-cases.json`). Python and Java tests must use these same files.

**Stage 2 (spec week 4, gate G1): done 2026-10-15. Plan kept below for reference.**
- Step 1 `services/ingestion-gateway`: **done 2026-10-14.** 59 tests pass (28 against Kafka via Testcontainers).
  Live run: 42,000 events accepted = 42,000 records in `telemetry.raw.v1`, no duplicates. Runs as a jar on the
  host; not in compose yet (step 4). Decisions in `docs/adr/0001-ingestion-gateway-skeleton.md`. Kafka record
  format: `libs/telemetry-contracts/schemas/telemetry-raw-event.1.0.json`.
- Step 2 `services/detection-service` (pass-through stub): **done 2026-10-14.** 18 pytest tests pass (2 against
  the compose Kafka on throw-away topics). Live run: `telemetry.scored.v1` = `telemetry.raw.v1` = 84,000 records,
  lag 0, DLQ empty. Scored format: `libs/telemetry-contracts/schemas/telemetry-scored-event.1.0.json`
  (nested `prediction`, see ADR-0002). Its own venv: `services\detection-service\.venv`.
- Gateway default port is **8081** (config, simulator default `--url`, compose).
- Step 3 `services/analytics-service`: **done 2026-10-14.** 12 tests pass (7 against Kafka + TimescaleDB via
  Testcontainers, database built by the real init script). Live run, all services on the host: 126,000 scored
  records consumed -> 119,350 rows = 119,350 distinct eventIds in `telemetry.scored.v1` (6,650 simulator repeats
  ignored), lag 0; `GET /api/v1/analytics/instances/checkout-api-01/series` returns the points. Port 8082.
  Decisions: ADR-0003. Instance names are `<service>-01` .. `<service>-10`.
- Step 4 compose: **done 2026-10-15.** Dockerfiles per service (build context = repo root), all five containers
  healthy with `up -d --build --wait`, user 10001, read-only root fs. Stub health = heartbeat file. ADR-0004.
- Step 5 smoke test: **done 2026-10-15.** `bench/scripts/smoke_test.py --clean`: 21/21 checks from empty volumes
  (42,000 accepted = raw = scored = rows, 0 duplicates, lag 0, API returns the points). Verified it fails (exit 1)
  when the detection container is stopped mid-run, and that the backlog drains after restart.
- Still missing for G1 as the spec words it: the smoke test **in CI** (no CI pipeline exists yet).
- Next: spec 16.2 week 5 (gateway hardening + observability baseline). Plan it with Leith first.

Later stages follow spec 16.2 (week 5: gateway hardening; week 6: data foundation; weeks 9-13: detection and ML; ...).

## Commands (PowerShell, from this folder)
```powershell
# whole stack (infrastructure + gateway :8081 + detection stub + analytics :8082)
docker compose -f infra/compose/docker-compose.yml --profile core --env-file .env up -d --build --wait
docker compose -f infra/compose/docker-compose.yml --profile core logs -f analytics-service
# end-to-end smoke test; --clean DELETES the stack's Kafka + database volumes first
simulator\.venv\Scripts\python bench\scripts\smoke_test.py --clean

# infrastructure only (when running services on the host instead)
docker compose -f infra/compose/docker-compose.yml --profile core --env-file .env up -d --wait kafka timescaledb
docker compose -f infra/compose/docker-compose.yml --profile core --env-file .env up kafka-init
docker compose -f infra/compose/docker-compose.yml --profile core down          # stop, keep data
docker exec telemetry-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
docker exec -it telemetry-timescaledb psql -U postgres -d analytics_db

# gateway (from services\ingestion-gateway\, JAVA_HOME set to JDK 21, Docker running for the tests)
mvn test
mvn package -DskipTests; & "$env:JAVA_HOME\bin\java" -jar target\ingestion-gateway-0.1.0.jar

# detection stub (from services\detection-service\)
.venv\Scripts\python -m pytest -v
.venv\Scripts\python -m detection_service

# analytics (from services\analytics-service\, JAVA_HOME set to JDK 21, Docker running for the tests)
mvn test
mvn package -DskipTests; & "$env:JAVA_HOME\bin\java" -jar target\analytics-service-0.1.0.jar
curl.exe "http://localhost:8082/api/v1/analytics/instances/checkout-api-01/series?metric=cpuUsagePercent&from=2026-10-14T12:00:00Z&to=2026-10-14T15:00:00Z"

# simulator (from simulator\)
.venv\Scripts\python -m unittest discover -s tests -v
.venv\Scripts\python -m fleet_sim describe
.venv\Scripts\python -m fleet_sim live --speed 60 --duration 30     # needs the gateway (port 8081)
```
Kafka from the host: `localhost:9092`. From containers: `kafka:19092`.
PostgreSQL: `localhost:5432`; users and dev passwords are in `.env`.

## Rules
1. **Contracts first.** Change `libs/telemetry-contracts` before code. Never edit
   `telemetry-batch.1.0.json` in a breaking way; make a new version file instead.
   Kafka message shapes (raw and scored events) also go in the contracts library.
2. **Database per service.** A service only connects to its own database with its own role.
3. Topic names and settings live in `infra/compose/kafka/create-topics.sh`. Auto-creation is off.
4. Use the technologies of spec 6.5; do not swap them without asking.
5. Every service gets automated tests, and tests must be able to fail (check this when it matters).
6. Record significant decisions as short ADRs in `docs/adr/`.
7. Keep `README.md`'s "Honest status" table true: what was verified and how.

## Stage 2 plan: walking skeleton (spec 16.2, week 4)
Goal (gate G1): **simulator -> gateway -> Kafka -> pass-through stub -> analytics writer ->
TimescaleDB -> query endpoint**, end to end, with a smoke test.
Build in this order and get each step running before the next.

1. `services/ingestion-gateway`: Java 21, Spring Boot 3, **WebFlux**, reactor-kafka,
   networknt json-schema-validator (spec 6.5).
   - `POST /api/v1/telemetry/ingest`, contract of spec 8.1, FR-ING-01 and FR-ING-03.
   - Validate with the JSON Schema in `libs/telemetry-contracts`, then the per-event semantic rules
     (timestamp window -24 h / +5 min, memoryUsed <= memoryLimit, duplicate eventId in batch) with
     the rejection codes in `libs/telemetry-contracts/README.md`. Reply 202 with accepted/rejected counts.
   - `X-API-Key` check, skeleton version: keys from configuration, compared as SHA-256 hashes.
     Simulator dev keys are `dev-agent-<service>` for agent `agent-<service>`.
   - Publish accepted events to `telemetry.raw.v1`, key = `instanceId`, idempotent producer,
     `acks=all` (FR-ING-07). Add `receivedAt` to each message (the database needs `received_at`).
   - Tests: every case of `invalid-cases.json`, plus one publishing test.
   - **Later, in week 5, not now:** agent registry in `ingestion_db` with R2DBC, rate limits (Bucket4j),
     `telemetry.rejected.v1`, BlockHound, gzip, sanitisation details.
2. `services/detection-service`: **pass-through stub** in Python 3.12 with confluent-kafka (spec 6.5).
   Consume `telemetry.raw.v1`, add `scoredAt` and a `prediction` object (spec 8.3: probability 0.0,
   isAnomaly false, modelVersion "stub-0", ...), and publish to `telemetry.scored.v1` (key = instanceId).
   The real model replaces it in weeks 9-13.
3. `services/analytics-service`: Java 21, Spring Boot 3 MVC with virtual threads, Spring Kafka,
   JdbcTemplate batch writes, Flyway (spec 6.5).
   - Flyway migration creating `telemetry_event` exactly as spec 8.4 (hypertable, primary key
     `(instance_id, event_time, event_id)`, indexes). Compression, retention and continuous
     aggregates can follow in week 6.
   - FR-ANA-01: consume `telemetry.scored.v1`, batch-insert (up to 500 events or 200 ms),
     `ON CONFLICT DO NOTHING`, commit Kafka offsets only after the database commit.
   - One minimal query endpoint from spec 8.2 so the data can be read back over HTTP.
4. Add the three services to `docker-compose.yml` (gateway on host port **8081**, analytics on **8082**).
5. End-to-end smoke test: run `fleet_sim live --speed 60 --duration 30`, then show that the row count
   in `telemetry_event` equals the simulator's `accepted` count, with no duplicates, and that the
   query endpoint returns the data.
   **Careful:** the simulator is deterministic (seed 42), so two runs whose time windows overlap re-send the
   same eventIds (seen on 2026-10-14: 6,650 repeats in Kafka). Start from empty topics/table, or count the
   eventIds of this run only, or the row count will be lower than the sum of `accepted`.

## Open questions for the supervisor
- Dead-letter topic naming: spec 8.3 says `<topic>.dlq.v1`, FR-DET-10 says `telemetry.raw.v1.dlq`.
  Currently FR-DET-10's form is used.
