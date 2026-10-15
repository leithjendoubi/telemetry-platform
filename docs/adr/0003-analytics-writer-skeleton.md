# ADR-0003: Analytics writer and query skeleton

- Status: accepted
- Date: 2026-10-14
- Context: Stage 2, step 3 (spec 16.2 week 4, gate G1)

## Decisions

1. **Batch listener, commit after the database.** Spring Kafka batch listener with ack-mode BATCH: the container
   commits offsets only when the listener method returns, and it returns only after the insert transaction committed
   (FR-ANA-01). `max.poll.records=500`, `fetch.max.wait.ms=200`, `fetch.min.bytes=64 KiB` give "up to 500 events or 200 ms".
2. **Database errors: retry the same batch forever** (`DefaultErrorHandler` with unlimited fixed back-off, 2 s).
   Spec 6.8 asks that analytics stops committing while TimescaleDB is down; the lag grows and drains on recovery.
3. **Poison records go to `telemetry.scored.v1.dlq` from inside the listener**, synchronously, before the database
   write, so a broken record never blocks the partition and is never lost. Same DLQ headers as the detection stub.
4. **Incoming records are checked against `telemetry-scored-event.1.0.json`** (networknt, as in the gateway), plus
   range checks for the `integer`/`bigint` columns, because JSON Schema integers have no upper bound.
5. **No `reWriteBatchedInserts` yet.** It would speed up inserts but makes the driver report "success, no info" per row,
   hiding the inserted/duplicate counts used to verify the skeleton. Revisit in week 6 (data foundation) with measurements.
6. **One query endpoint**, the instance series of spec 8.2, raw points with the 2,000-point hard limit of FR-ANA-03
   and `truncated` flag; contract first in `libs/telemetry-contracts/openapi/analytics-api.yaml`. Metric names are
   mapped to columns through a fixed allow-list, so no request text reaches SQL. JWT security comes with Keycloak.
7. **Port 8082.**
8. **Integration tests start TimescaleDB with the real `infra/compose/init-db/01-create-databases.sh`**, so the
   test database has the same databases, roles and extension as compose, and the init script is tested too.
