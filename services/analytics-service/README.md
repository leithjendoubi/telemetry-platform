# analytics-service

Stores every scored event in TimescaleDB and lets you read it back over HTTP.

Java 21, Spring Boot 3.5 MVC on virtual threads, Spring Kafka, JdbcTemplate batch writes, Flyway (spec 6.5).
Port **8082** (8080 = Pi-hole on the dev machine, 8081 = gateway).

## What it does today (Stage 2, step 3)

**Writer (FR-ANA-01).** Consumer group `analytics-writer` reads `telemetry.scored.v1` in batches of up to 500
records (or whatever arrived within ~200 ms) and, for each batch:

1. parses each record against `telemetry-scored-event.1.0.json`; broken ones are copied to `telemetry.scored.v1.dlq`
   with the reason in headers (and Kafka must confirm that copy);
2. inserts the good ones into `telemetry_event` in **one transaction**, `ON CONFLICT DO NOTHING`, so a repeated event
   (same instance, time and eventId) is skipped instead of stored twice;
3. only then are the Kafka offsets committed.

If the database fails, the batch is retried every 2 s and nothing is committed: lag grows, nothing is lost, and the
rows arrive once the database is back (spec 6.8). A stats line is logged every 10 s:
`consumed=... inserted=... duplicates=... dead_lettered=...`.

**Schema.** Flyway migration `V1__telemetry_event.sql` creates `telemetry_event` exactly as spec 8.4: hypertable on
`event_time` with 1-day chunks, primary key `(instance_id, event_time, event_id)`, the two indexes. Compression,
retention and continuous aggregates come in week 6 as `V2`, `V3`, ... (never edit `V1` once applied).

**Query (spec 8.2, contract in `libs/telemetry-contracts/openapi/analytics-api.yaml`):**

```
GET /api/v1/analytics/instances/{instanceId}/series?metric=cpuUsagePercent&from=2026-10-14T12:50:00Z&to=2026-10-14T14:30:00Z
```

Returns the raw points of [from, to) in time order with their anomaly score, at most 2,000 (`truncated: true` if
more exist), window at most 7 days. Errors are RFC 9457 problem details: 400 for bad input, 503 when the database is
down. Instance names are `<service>-01` to `<service>-10`.

Probes: `/health/live`, `/health/ready` (ready includes the database).

**Not yet:** automatic granularity, cursor pagination, the other endpoints of spec 8.2 (week 14), JWT security
(Keycloak), SSE live channel, bulk loader, caching, Prometheus metrics.

## Build, test, run (PowerShell)

```powershell
cd services\analytics-service
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot"

mvn test                       # 12 tests, about 1 minute; Docker must be running
mvn package -DskipTests
# needs Kafka + TimescaleDB from infra/compose running
& "$env:JAVA_HOME\bin\java" -jar target\analytics-service-0.1.0.jar
```

Settings via environment variables: `KAFKA_BOOTSTRAP_SERVERS`, `POSTGRES_HOST`, `POSTGRES_PORT`,
`ANALYTICS_DB_USER`, `ANALYTICS_DB_PASSWORD` (defaults: the local dev values of `.env.example`).

## Tests

| Test class | What it proves |
|-----------|----------------|
| `ScoredEventParserTest` | every column is mapped from the contract example; broken records and values too large for the columns are poison |
| `AnalyticsIntegrationTest` | real Kafka + real TimescaleDB, the database prepared by **the real `infra/compose/init-db` script**: the table is exactly spec 8.4 (hypertable, 1-day chunks, primary key order, indexes, owner `analytics`); 50 events + 10 replayed copies give exactly 50 rows with the right values; poison goes to the DLQ; offsets are committed; during a database outage nothing is committed and the rows arrive afterwards; the query endpoint (order, exclusive `to`, 2,000-point limit, 400 problem details); probes |
