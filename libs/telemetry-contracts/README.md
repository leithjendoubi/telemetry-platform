# telemetry-contracts

The shared **published language** of the platform (spec 7.1): the schemas every service agrees on.
It contains data shapes only, never business logic.

| Path | What it is |
|------|------------|
| `schemas/telemetry-batch.1.0.json` | JSON Schema (draft 2020-12) of the ingest request body. Normative, copied from spec 8.1. |
| `examples/valid-batch.json` | The example payload of spec 8.1. Must always validate. |
| `examples/invalid-cases.json` | Table of broken variants of the valid example, each with the layer that must reject it. |
| `schemas/telemetry-raw-event.1.0.json` | Value of one Kafka record on `telemetry.raw.v1` (spec 8.3): one accepted event plus `receivedAt`, `agentId`, `batchId`. |
| `examples/raw-event.json` | Example record of `telemetry.raw.v1`. Must always validate. |
| `schemas/telemetry-scored-event.1.0.json` | Value of one Kafka record on `telemetry.scored.v1` (spec 8.3, FR-DET-03): one raw record plus `prediction` and `scoredAt`. |
| `examples/scored-event.json` | Example record of `telemetry.scored.v1` (pass-through stub values). Must always validate. |
| `openapi/analytics-api.yaml` | OpenAPI 3.1 of the analytics query API (spec 8.2). Stage 2: one endpoint, the series of one instance. |

## Kafka record of `telemetry.raw.v1` (produced by the ingestion gateway)

- **Key:** `resource.instanceId` (UTF-8 string), so all events of one instance stay in order on one partition.
- **Value:** UTF-8 JSON matching `telemetry-raw-event.1.0.json`. The event fields are copied unchanged from the request.
- **Headers (spec 8.3 envelope):** `event-type` = `TelemetryReceived`, `schema-version` = `1.0`,
  `produced-at` (RFC 3339, UTC), `producer` = `ingestion-gateway/<version>`. `traceparent` is added with OpenTelemetry in week 5.

## Rules

1. **Contracts first.** Change the schema before changing code (spec principle 5).
2. **No breaking change without a version bump.** Adding a required field or tightening a limit means a new file `telemetry-batch.1.1.json` or `2.0.json`, never an edit of `1.0`.
3. The gateway (Java) and the simulator (Python) test against these same files, so the two sides cannot drift apart (risk R-08).

## What JSON Schema cannot check

These rules of spec 8.1 are applied by code after schema validation, one event at a time:

| Rule | Rejection code |
|------|----------------|
| timestamp older than 24 h or more than 5 min in the future | `TIMESTAMP_OUT_OF_RANGE` |
| `memoryUsedBytes` greater than `memoryLimitBytes` | `SEMANTIC_VIOLATION` |
| duplicate `eventId` inside one batch | `DUPLICATE_EVENT_ID` |
| `serviceName` not in the agent's allow-list | `SERVICE_NOT_PERMITTED` |
| any schema violation of the event | `SCHEMA_VIOLATION` |

## Kafka record of `telemetry.scored.v1` (produced by the detection service)

- **Key:** `resource.instanceId`, same as the raw topic.
- **Value:** UTF-8 JSON matching `telemetry-scored-event.1.0.json`: the raw record unchanged, plus
  `prediction` = `{probability, isAnomaly, threshold, modelVersion, featureSetVersion, warmup}` and `scoredAt`.
- **Headers:** `event-type` = `TelemetryScored`, `schema-version` = `1.0`, `produced-at`, `producer` = `detection-service/<version>`;
  `traceparent` is copied from the raw record when present.
- The pass-through stub (until weeks 9-13) always sends `probability` 0.0, `isAnomaly` false, `threshold` 0.5,
  `modelVersion` `stub-0`, `featureSetVersion` `none`, `warmup` false.

## Dead-letter record of `telemetry.raw.v1.dlq`

A raw record the detection service cannot process is copied there unchanged (same key, value and headers), with extra headers
`dlq-error` (reason), `dlq-attempts`, `dlq-source-topic`, `dlq-source-partition`, `dlq-source-offset` (spec 8.3: "original record plus error and attempt count").
