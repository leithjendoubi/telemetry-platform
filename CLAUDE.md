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
- Ask before big design decisions (new technology, changing the contract).
- Commands: Leith lets you run them without asking, including ones that delete data or publish
  (`docker compose down -v`, `smoke_test.py --clean`, `git push`). Afterwards, tell him what you ran and
  what it changed, and name every command that deleted data or published something.
- Blocked on purpose (`.claude/settings.local.json` deny list): discarding uncommitted work (`git reset --hard`,
  `git clean`, `git checkout -- .`, `git restore`), force pushes, and any docker command on the Pi-hole container.
  If one of these seems necessary, explain why to Leith instead of working around the block.

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
- Still missing for G1 as the spec words it: the smoke test **in CI**. A pipeline exists but is uncommitted
  and has never run (see the handoff below).
- Next: spec 16.2 week 5 (gateway hardening + observability baseline). Plan it with Leith first.

**Handoff 2026-11-05 (audit done in a claude.ai chat; continue from here).**
Uncommitted since commit 83d0844 (15 Oct): CI pipeline `.github/workflows/ci.yml` (spec 14.4 stages 1-6); root `pom.xml`
(parent POM: versions + Spotless/Checkstyle/Failsafe/JaCoCo/Dependency-Check); `config/checkstyle`, `config/dependency-check`;
integration tests renamed `*Test` -> `*IT` (run by Failsafe in `mvn verify`); ArchUnit `ArchitectureTest` in both Java
services; `AgentCredential` (auth no longer imports config); import-linter contract and ruff/mypy settings in both Python
projects; reformatting by Spotless/ruff. No behaviour change except: the simulator rejects non-http(s) URLs, and the smoke
test only opens `http://localhost:` URLs. Simulator data logic unchanged (seed 42 gives the same data).
- Verified on Windows 2026-11-05: `mvn spotless:check checkstyle:check` 0 violations; `mvn verify` from the root green
  (gateway 33 unit + 30 IT, analytics 10 unit + 7 IT; coverage gateway 92.2 % lines / 85.9 % branches, analytics
  94.7 % / 80.0 %); simulator pytest 56 passed + 1 expected skip, ruff/format/mypy clean; smoke script ruff/format/mypy
  clean; detection ruff/format/mypy/lint-imports clean.
- Detection pytest failed once (1 of 18, then coverage 79 %): `committed_total` in `tests/test_kafka_flow.py` crashed on
  NOT_COORDINATOR from a broker started seconds earlier (CI stage 5 does exactly that). Fixed: coordinator-not-ready codes
  now count as "not yet" and `wait_for` keeps polling. **Re-verified 2026-11-05**: 3 runs, each right after
  `docker restart telemetry-kafka` + `up -d --wait kafka`: 18 passed, coverage 85.28 %. The same "-1" case was also guarded
  in `test_output_refused_means_no_commit` (wait for a real answer before asserting 0); 4th run after a restart green;
  ruff 0.16.9 (the pinned version), format, mypy, lint-imports clean.
- Open items from the audit (fix before or during the first CI run):
  1. No OpenAPI validation test, although `openapi-spec-validator` is a simulator dev dependency (spec 14.4 stage 1).
  2. Dependency-Check runs only if the GitHub secret `NVD_API_KEY` exists; without it stage 3 is green without scanning Java.
  3. `repo.root` = `${project.basedir}/../..` is right for the services but wrong for the root project, so the `aggregate`
     scan's suppression file path points outside the repository.
  4. Likely first-run CI failures: Trivy findings in base images. Settled: the ruff mismatch (venv has the pinned
     0.16.9); Gitleaks (35 hits = the SHA-256 of the public dev keys, checked; `.gitleaks.toml` excuses only 64-hex values
     in `dev-agents.yml` for the generic-api-key rule; planted GitHub token and non-hex key in that file still caught).
  5. The detection coverage gate needs the compose Kafka running (the Kafka tests cover the service loop).
  6. No ADR yet for the CI pipeline and parent POM (rule 6): write ADR-0005.
  7. No Git remote yet; only branch `main`.
- Next: (done 2026-11-05: detection fix re-verified, this file and README's "Honest status" updated);
  propose a commit split and **wait for Leith's OK** before committing; create the GitHub remote, push, get every CI
  stage green; then plan spec 16.2 week 5 and **wait for approval** before coding it.

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

# both Java services at once (from this folder, JAVA_HOME set to JDK 21, Docker running for the *IT tests)
mvn spotless:check checkstyle:check   # format + lint; mvn spotless:apply fixes formatting
mvn test                              # unit + ArchUnit tests only (Surefire)
mvn verify                            # + integration tests *IT (Failsafe, Testcontainers) + JaCoCo coverage gates

# gateway (from services\ingestion-gateway\; mvn test / mvn verify work here too, for this service only)
mvn package -DskipTests; & "$env:JAVA_HOME\bin\java" -jar target\ingestion-gateway-0.1.0.jar

# detection stub (from services\detection-service\; the Kafka tests need the compose Kafka, else they skip
# and the 80 % coverage gate fails)
.venv\Scripts\python -m pytest -q
.venv\Scripts\python -m ruff check .; .venv\Scripts\python -m ruff format --check .; .venv\Scripts\python -m mypy; .venv\Scripts\lint-imports
.venv\Scripts\python -m detection_service

# analytics (from services\analytics-service\)
mvn package -DskipTests; & "$env:JAVA_HOME\bin\java" -jar target\analytics-service-0.1.0.jar
curl.exe "http://localhost:8082/api/v1/analytics/instances/checkout-api-01/series?metric=cpuUsagePercent&from=2026-10-14T12:00:00Z&to=2026-10-14T15:00:00Z"

# simulator (from simulator\)
.venv\Scripts\python -m pytest -q                    # what CI runs (56 passed, 1 expected skip)
.venv\Scripts\python -m ruff check .; .venv\Scripts\python -m ruff format --check .; .venv\Scripts\python -m mypy
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
