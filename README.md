# Telemetry Platform (PFE CAP-PFE-2026-TEL-084)

Event-driven telemetry ingestion and real-time predictive anomaly detection platform.
The authoritative definition of what to build is the Engineering Project Specification (EPS) Rev 2.0.
Section numbers like "spec 8.1" below refer to it.

## What is in this repository today (Stage 1 and Stage 2 done: walking skeleton, gate G1)

| Part | Status | Where |
|------|--------|-------|
| Monorepo layout (spec 14.1) | done | whole tree; empty folders hold a `.gitkeep` |
| Kafka (KRaft) + PostgreSQL/TimescaleDB with 3 logical databases, Docker Compose | done, runs on Windows | `infra/compose/` |
| All Kafka topics of the event catalogue (spec 8.3) | done, 10 topics created | `infra/compose/kafka/create-topics.sh` |
| Telemetry contract: JSON Schema v1.0 + valid/invalid examples, Kafka records of `telemetry.raw.v1` and `telemetry.scored.v1` | done, tested (Python and Java) | `libs/telemetry-contracts/` |
| Fleet simulator v0 (normal traffic, offline + live mode) | done, tested | `simulator/` |
| Ingestion gateway, skeleton version (validation, API keys, publish to Kafka) | done, tested (Stage 2 step 1) | `services/ingestion-gateway/` |
| Detection service, pass-through stub (raw -> scored, DLQ for poison records) | done, tested (Stage 2 step 2) | `services/detection-service/` |
| Analytics service: writer into the TimescaleDB hypertable (spec 8.4, Flyway) + one query endpoint | done, tested (Stage 2 step 3) | `services/analytics-service/` |
| Alerting, dashboard | **not started** | `services/` |

Roadmap position: Weeks 1 to 3 of spec 16.2 (scaffold, contracts, simulator v0). The walking skeleton
(Week 4) is finished in Stage 2 with the gateway and the analytics writer.

## Quick start (PowerShell, from the repository root)

You need: Docker Desktop (engine running), Python 3.12 for the simulator (`simulator\.venv`), and Java 21 + Maven
only if you build or test the Java services outside Docker.

```powershell
# 1. Whole stack: Kafka, TimescaleDB, ingestion gateway (:8081), detection stub, analytics service (:8082)
docker compose -f infra/compose/docker-compose.yml --profile core --env-file .env up -d --build --wait

# 2. Send traffic (30 s at 60x speed = 30 minutes of fleet data)
simulator\.venv\Scripts\python -m fleet_sim live --speed 60 --duration 30

# 3. Read it back
curl.exe "http://localhost:8082/api/v1/analytics/instances/checkout-api-01/series?metric=cpuUsagePercent&from=2026-10-15T00:00:00Z&to=2026-10-16T00:00:00Z"

# End-to-end smoke test (gate G1). --clean DELETES the stack's Kafka and database data first.
simulator\.venv\Scripts\python bench\scripts\smoke_test.py --clean

# Stop
docker compose -f infra/compose/docker-compose.yml --profile core down       # keeps data
docker compose -f infra/compose/docker-compose.yml --profile core down -v    # deletes data volumes too
```

Each folder has its own README with its tests: `simulator/`, `services/ingestion-gateway/`,
`services/detection-service/`, `services/analytics-service/`, `libs/telemetry-contracts/`.
On Linux/CI the `Makefile` offers the same as targets (`make up`, `make smoke`, `make help`).

## Honest status: what was and was not verified

| Item | Verified how |
|------|--------------|
| Simulator logic, contract, live mode | 55 automated tests pass on Windows (2026-10-14). The 1 skipped test checks the "pyarrow missing" message and can only run without pyarrow. A full day (2,016,000 rows, 350 instances, 0 duplicate IDs) was generated and counted independently from the file |
| Tests can actually fail | Deliberate bugs were planted (retry logic, contract limit, seed handling); the tests caught them. One planted bug (CPU clamp raised to 140%) was not caught because normal data never reaches 100% CPU |
| Parquet output | Test passes on Leith's Windows machine with pyarrow installed; same seed gives bit-identical data on Linux and Windows (fingerprint check) |
| `docker-compose.yml`, init scripts, topics | Containers start healthy on Docker Desktop (Windows), 3 databases created, 10 topics created by `kafka-init` |
| Ingestion gateway | 59 automated tests pass (`mvn test`, Java 21), 28 of them against a real Kafka in Docker (Testcontainers). Planted bugs (format checks off, future-timestamp rule removed, wrong Kafka key) were caught. Live run on Windows: `fleet_sim live --speed 60 --duration 30` gave 4,200 batches, all 202, 42,000 events accepted; Kafka `telemetry.raw.v1` then held exactly 42,000 records, 42,000 distinct eventIds, every key equal to its instanceId |
| Detection stub | 18 pytest tests pass, 2 of them against the compose Kafka on throw-away topics. Planted bugs (commit despite refused outputs, poison records dropped) were caught. Live run gateway -> stub: `telemetry.scored.v1` held exactly as many records as `telemetry.raw.v1` (84,000), consumer lag 0, DLQ empty |
| Analytics service | 12 automated tests pass, 7 against a real Kafka and a real TimescaleDB (Testcontainers; the database is prepared by the real `init-db` script). Planted bugs (no `ON CONFLICT DO NOTHING`, database errors swallowed so offsets get committed) were caught. Live run of the whole chain on Windows: 126,000 scored records consumed, 119,350 rows stored = 119,350 distinct eventIds in `telemetry.scored.v1` (the other 6,650 were repeats of the deterministic simulator and were skipped), consumer lag 0; the query endpoint returned the 341 points of `checkout-api-01` |
| Services in Docker Compose | Images build (multi-stage, layered jar / slim Python, non-root user 10001, HEALTHCHECK); the whole stack comes up healthy with `up --wait`. Checked inside the containers: user id 10001, root file system read-only, only `/tmp` writable |
| End-to-end smoke test (gate G1) | `bench/scripts/smoke_test.py --clean` on Windows, 2026-10-15, from empty volumes: **21/21 checks passed** (42,000 accepted = 42,000 records on each topic = 42,000 rows, 0 duplicates, 0 in the DLQs, lag 0, query endpoint returns the stored points, all services healthy). Passed twice. It can fail: with the detection container stopped mid-run it reported 17/21 (scored 32,470, lag 9,530) and exit code 1; after restarting the container the backlog drained to exactly 42,000 rows, 42,000 distinct |
| Smoke test in CI | **Not done**: there is no CI pipeline in the repository yet (spec 16.2 asks for the smoke test in CI) |
| Makefile | Targets updated, **not run** (no `make` on the Windows machine) |
| Shell scripts | `bash -n` syntax check, and they ran successfully inside the containers |

## Things to know

- **Dead-letter topic names.** The spec says `<topic>.dlq.v1` in 8.3 but `telemetry.raw.v1.dlq` in FR-DET-10. The script follows FR-DET-10. Ask the supervisor which form he wants; it is a one-line change.
- **Image versions** are pinned by tag (`apache/kafka:3.8.1`, `timescale/timescaledb:2.17.2-pg16`). Spec 14.2 asks for digests; that comes with the version lock table (`docs/TECH_STACK.md`) at Gate G0.
- **Dev passwords** in `.env.example` are for local use only.
- **Simulator reproducibility** holds for the same seed, config and NumPy version. The manifest records the versions.

## Stage 2 (walking skeleton, spec week 4)

1. ~~Ingestion gateway (Java 21, Spring WebFlux): validation against the contract, API-key check, publish to `telemetry.raw.v1`.~~ Done, see `services/ingestion-gateway/README.md`.
2. ~~Analytics writer (Spring Boot): consume, batch-insert into a TimescaleDB hypertable.~~ Done, see `services/analytics-service/README.md`.
3. ~~A pass-through stub standing in for detection, then an end-to-end smoke test: simulator -> gateway -> Kafka -> database row -> query.~~ Done: `services/detection-service/`, `infra/compose/docker-compose.yml`, `bench/scripts/smoke_test.py`.

**Next (spec 16.2 week 5):** gateway hardening (agent registry in `ingestion_db`, rate limits, rejected topic, gzip, BlockHound) and the observability baseline. A CI pipeline running the smoke test is still missing.
