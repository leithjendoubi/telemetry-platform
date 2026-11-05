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
| `schemas/telemetry-rejected-event.1.0.json` | Value of one Kafka record on `telemetry.rejected.v1` (spec 8.3, FR-ING-07): one rejected event with its reason code. |
| `examples/rejected-event.json` | Example record of `telemetry.rejected.v1`. Must always validate. |
| `openapi/analytics-api.yaml` | OpenAPI 3.1 of the analytics query API (spec 8.2). Stage 2: one endpoint, the series of one instance. |

## Kafka record of `telemetry.raw.v1` (produced by the ingestion gateway)

- **Key:** `resource.instanceId` (UTF-8 string), so all events of one instance stay in order on one partition.
- **Value:** UTF-8 JSON matching `telemetry-raw-event.1.0.json`. The event fields are copied unchanged from the request,
  except `timestamp`, which is always in UTC (FR-ING-04): one sent with an offset (`+02:00`) arrives as the same instant
  with `Z` and the same number of fraction digits; one already in UTC is kept exactly as sent.
- **Headers (spec 8.3 envelope):** `event-type` = `TelemetryReceived`, `schema-version` = `1.0`,
  `produced-at` (RFC 3339, UTC), `producer` = `ingestion-gateway/<version>`. `traceparent` is added with OpenTelemetry in week 5.

## Rules

1. **Contracts first.** Change the schema before changing code (spec principle 5).
2. **No breaking change without a version bump.** Adding a required field or tightening a limit means a new file `telemetry-batch.1.1.json` or `2.0.json`, never an edit of `1.0`.
3. The gateway (Java) and the simulator (Python) test against these same files, so the two sides cannot drift apart (risk R-08).
4. **Patterns are ECMA-262**, as JSON Schema requires: `$` means the very end of the text. Java's and Python's regex engines
   also let `$` match before a final newline, so `"checkout-api-07\n"` would pass `^[A-Za-z0-9]...$`. Every validator of these
   schemas must read `$` as the true end (the gateway and the Python tests translate it to `\z` / `\Z`).

## What JSON Schema cannot check

These rules of spec 8.1 are applied by code after schema validation, one event at a time:

| Rule | Rejection code |
|------|----------------|
| timestamp older than 24 h or more than 5 min in the future | `TIMESTAMP_OUT_OF_RANGE` |
| `memoryUsedBytes` greater than `memoryLimitBytes` | `SEMANTIC_VIOLATION` |
| a metric too large for a 64-bit float (e.g. `1e400`, which parsers turn into Infinity; FR-ING-04) | `SEMANTIC_VIOLATION` |
| duplicate `eventId` inside one batch | `DUPLICATE_EVENT_ID` |
| `serviceName` not in the agent's allow-list | `SERVICE_NOT_PERMITTED` |
| any schema violation of the event | `SCHEMA_VIOLATION` |

## Kafka record of `telemetry.rejected.v1` (produced by the ingestion gateway)

- **One record per rejected event** of a batch whose envelope was valid (a batch rejected as a whole with 400/401/403/413/415
  produces no record: there is no trustworthy event to report).
- **Key:** `agentId` (spec 8.3), so the audit trail of one agent stays in order.
- **Value:** UTF-8 JSON matching `telemetry-rejected-event.1.0.json`: `index`, `eventId`, `code`, `message` exactly as in the 202
  response's `rejections` list, plus `agentId`, `batchId`, `receivedAt`, and `event` = the event exactly as sent.
  Consumers must treat `event` and `eventId` as untrusted input.
- **Headers:** `event-type` = `TelemetryRejected`, `schema-version` = `1.0`, `produced-at`, `producer` = `ingestion-gateway/<version>`.

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
