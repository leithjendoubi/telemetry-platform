# ADR-0004: Service images, Compose wiring and the end-to-end smoke test

- Status: accepted
- Date: 2026-10-15
- Context: Stage 2, steps 4 and 5 (spec 16.2 week 4, gate G1)

## Decisions

1. **Images are built from the repository root** (`build.context: ../..`), because every service carries
   `libs/telemetry-contracts`. A root `.dockerignore` keeps venvs, `target/`, data, docs and `.env` out of the context.
2. **Image shape per spec 14.2.** Java: Maven build stage, Spring Boot layered jar (`-Djarmode=tools extract --layers`),
   `eclipse-temurin:21.0.12.1_1-jre` runtime, `-XX:MaxRAMPercentage=60`. Python: venv built in a first stage,
   `python:3.12.15-slim-trixie` runtime. All run as user 10001, have a `HEALTHCHECK`, and run in Compose with
   `read_only: true` and a tmpfs `/tmp`. Tags are pinned to exact versions; digests follow with the G0 lock table.
   Tests do not run inside `docker build` (they need Docker themselves); they run on the host or in CI before the build.
3. **Detection stub health = heartbeat file.** The stub has no HTTP port until the real FastAPI service; its loop
   touches `/tmp/heartbeat` on every pass and the health check fails if the file is older than 30 s (a stuck loop).
4. **Start order through `depends_on`:** every service waits for a healthy Kafka and a successfully finished
   `kafka-init` (topics exist, auto-creation is off); analytics also waits for a healthy TimescaleDB.
   `restart: unless-stopped`, so the stub comes back after its deliberate exit on refused outputs.
5. **Host ports 8081 (gateway) and 8082 (analytics)**, configurable as `GATEWAY_PORT` / `ANALYTICS_PORT` in `.env`.
6. **Smoke test = reconciliation script** in `bench/scripts/smoke_test.py` (spec 14.1: "reconciliation" scripts),
   Python standard library only, run with the simulator's interpreter so it works on Windows and Linux CI alike.
   It reads Kafka and PostgreSQL through `docker exec`, needing no client libraries. It refuses to run on a stack that
   already holds data unless `--clean` (`down -v`) is given, because the deterministic simulator makes leftover data
   collide with new runs (see CLAUDE.md, step 5 note).
