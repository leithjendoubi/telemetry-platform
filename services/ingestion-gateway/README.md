# ingestion-gateway

The front door of the platform. Monitoring agents (here: the simulator) `POST` batches of telemetry events;
the gateway checks them and writes the good ones to Kafka topic `telemetry.raw.v1`.

Java 21, Spring Boot 3.5 WebFlux, reactor-kafka, networknt json-schema-validator (spec 6.5).

## What it does today (Stage 2, step 1: walking-skeleton version)

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
| valid events written to Kafka (`acks=all`, idempotent producer, key = `instanceId`) | **202** with `accepted`, `rejected`, `rejections[]` |
| Kafka does not acknowledge within ~10 s | 503 with `Retry-After: 5` |

Errors are RFC 9457 problem details (`application/problem+json`). `X-Request-Id` is echoed.
The Kafka record format is in `libs/telemetry-contracts` (`telemetry-raw-event.1.0.json` and its README).

Probes: `GET /health/live`, `GET /health/ready`.

**Not yet (week 5, spec 16.2):** agent registry in `ingestion_db`, rate limits, `telemetry.rejected.v1`, gzip,
BlockHound, full sanitisation, Prometheus `/metrics`, OpenTelemetry `traceparent`, Kafka check in readiness.

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

mvn test                       # 59 tests, about 1 minute (pulls apache/kafka:3.8.1 the first time)
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
| `IngestControllerTest` | the whole service over HTTP with a real Kafka (Testcontainers): every invalid case again, status codes, record key/value/headers, the record matches `telemetry-raw-event.1.0.json`, nothing is published for fully rejected batches |
| `KafkaUnavailableTest` | Kafka down gives 503 + `Retry-After`, never 202 |

Testcontainers is pinned to 1.21.4 in `pom.xml`: 1.21.3 cannot talk to Docker Engine 29.
