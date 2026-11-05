# ingestion-gateway

The front door of the platform. Monitoring agents (here: the simulator) `POST` batches of telemetry events;
the gateway checks them, writes the good ones to Kafka topic `telemetry.raw.v1` and records the bad ones, with their
reason code, on `telemetry.rejected.v1`.

Java 21, Spring Boot 3.5 WebFlux, reactor-kafka, networknt json-schema-validator (spec 6.5).

## What it does today (walking skeleton + week 5 in progress)

`POST /api/v1/telemetry/ingest` (spec 8.1). Checks, in order; the first failure decides the answer:

| Check | Answer |
|-------|--------|
| `X-API-Key` missing or unknown | 401 |
| `Content-Type` is not `application/json`, or a `Content-Encoding` is sent | 415 |
| body larger than 256 KiB | 413 |
| body is not JSON (NaN and Infinity are refused) | 400 |
| more than 100 events | 413 |
| envelope (everything outside `events`) breaks `telemetry-batch.1.0.json` | 400, with the list of errors |
| `agent.agentId` is not the agent that owns the key | 403 |
| each event: schema, allowed service, timestamp window (-24 h / +5 min), memoryUsed <= memoryLimit, duplicate eventId | event rejected with its code, the others carry on |
| valid events to `telemetry.raw.v1` (key = `instanceId`), rejected ones to `telemetry.rejected.v1` (key = `agentId`); `acks=all`, idempotent producer | **202** with `accepted`, `rejected`, `rejections[]`, once Kafka acknowledged every record |
| Kafka does not acknowledge within ~10 s | 503 with `Retry-After: 5` (also for a fully rejected batch: its rejections must be recorded, ADR-0007) |

Errors are RFC 9457 problem details (`application/problem+json`). `X-Request-Id` is echoed. A rejection message is at
most 1000 characters (longer ones are cut and end with `[...]`).
Kafka record formats are in `libs/telemetry-contracts` (`telemetry-raw-event.1.0.json`, `telemetry-rejected-event.1.0.json`
and its README).

Probes: `GET /health/live`, `GET /health/ready`.

**Not yet (week 5 plan in CLAUDE.md):** agent registry in `ingestion_db` (A3), rate limits (A4), full sanitisation (A5),
BlockHound (A6), gzip (A7), Prometheus `/metrics` and OpenTelemetry `traceparent` (B1, B2), Kafka check in readiness.

## API keys (skeleton)

`src/main/resources/dev-agents.yml` holds the 35 simulator agents: agent `agent-<service>` has key
`dev-agent-<service>` and may only send events for `<service>`. Only the SHA-256 of each key is stored.
To compute the hash of a new key in PowerShell:

```powershell
$k = "dev-agent-checkout-api"
-join ([Security.Cryptography.SHA256]::Create().ComputeHash([Text.Encoding]::UTF8.GetBytes($k)) | ForEach-Object { $_.ToString("x2") })
```

## Build, test, run (PowerShell)

Java 17 is still your default `java`, so point `JAVA_HOME` at Java 21 for this window first.
The integration tests start their own Kafka in Docker, so Docker Desktop must be running.

```powershell
cd services\ingestion-gateway
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot"

mvn test                       # unit + ArchUnit tests, seconds, no Docker
mvn verify                     # + integration tests *IT against Kafka in Docker and the coverage gate, ~1 minute
mvn package -DskipTests        # builds target\ingestion-gateway-0.1.0.jar

# needs Kafka from infra/compose running (see the main CLAUDE.md commands)
& "$env:JAVA_HOME\bin\java" -jar target\ingestion-gateway-0.1.0.jar
```

Port: **8081** (8080 is used by a Pi-hole on the dev machine). The simulator's default `--url` points there too.

Settings can be changed with environment variables, e.g. `KAFKA_BOOTSTRAP_SERVERS=kafka:19092` inside compose.

## Tests

| Test class | What it proves |
|-----------|----------------|
| `BatchValidatorTest` | every case of `invalid-cases.json` gets its expected code; duplicates, allow-list, partial acceptance, exact window edges, uuid/date-time formats |
| `ApiKeyAuthenticatorTest` | keys are matched by SHA-256; bad configuration stops start-up |
| `RejectedEventContractTest` | the `RejectionCode` enum equals the codes of `telemetry-rejected-event.1.0.json`; the Java validator accepts its example |
| `RejectionTest` | rejection messages are capped at 1000 characters without splitting a character |
| `ArchitectureTest` | package rules (ArchUnit), including the guard of ADR-0006 |
| `IngestControllerIT` | the whole service over HTTP with a real Kafka (Testcontainers): every invalid case again, status codes, record key/value/headers, records match their contract schemas, **AT-02** (mixed batch: raw, 202 body and rejected topic agree) |
| `KafkaUnavailableIT` | Kafka down gives 503 + `Retry-After`, never 202 |

Testcontainers must be 1.21.4 or newer (1.21.3 cannot talk to Docker Engine 29); Spring Boot 3.5.16 manages 1.21.4.
