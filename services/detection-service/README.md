# detection-service

**Stage 2: pass-through stub.** It reads every record from `telemetry.raw.v1`, adds a fixed "nothing unusual"
prediction and `scoredAt`, and writes the result to `telemetry.scored.v1`. The real model (FastAPI, scikit-learn,
rolling windows, episodes) replaces it in weeks 9-13 without changing the record shape.

Python 3.12, confluent-kafka (spec 6.5). Record shapes: `libs/telemetry-contracts`
(`telemetry-raw-event.1.0.json` in, `telemetry-scored-event.1.0.json` out; see its README).

## What it does

```
poll up to 500 records -> for each: check it against the raw contract
                             valid  -> raw record + prediction + scoredAt  -> telemetry.scored.v1 (key = instanceId)
                             broken -> copied unchanged, with the reason in headers -> telemetry.raw.v1.dlq
                       -> wait until Kafka acknowledged all outputs -> commit the input offsets
```

- **At-least-once:** offsets are committed only after the outputs are stored. If Kafka refuses an output, the stub
  stops without committing and exits with code 1; on restart it re-reads from the last commit. A record may be
  scored twice after a crash, and the analytics writer ignores the second copy (`ON CONFLICT DO NOTHING`).
- **Stub prediction:** `probability` 0.0, `isAnomaly` false, `threshold` 0.5 (placeholder), `modelVersion` `stub-0`,
  `featureSetVersion` `none`, `warmup` false.
- Consumer group `detection` (FR-DET-01). A new group starts at the oldest retained record.
- A stats line is logged every 10 s: `consumed=... scored=... dead_lettered=...`.

**Not in the stub:** HTTP endpoints `/v1/predict`, `/health`, `/ready`, `/metrics` (FR-DET-05), model loading,
rolling windows, warmup flags, episodes. They come with the real service.

## Settings (environment variables)

| Variable | Default |
|----------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` (`kafka:19092` inside compose) |
| `DETECTION_GROUP_ID` | `detection` |
| `DETECTION_INPUT_TOPIC` / `DETECTION_OUTPUT_TOPIC` / `DETECTION_DLQ_TOPIC` | `telemetry.raw.v1` / `telemetry.scored.v1` / `telemetry.raw.v1.dlq` |
| `DETECTION_MAX_BATCH` | `500` |
| `CONTRACTS_DIR` | the repository's `libs/telemetry-contracts` |

## Set up, test, run (PowerShell, from this folder)

```powershell
py -3.12 -m venv .venv                          # once
.venv\Scripts\python -m pip install -e ".[dev]" # once

.venv\Scripts\python -m pytest -v               # 18 tests; the 2 Kafka tests need the compose Kafka running
.venv\Scripts\python -m detection_service       # runs until Ctrl+C
```

## Tests

| File | What it proves |
|------|----------------|
| `tests/test_scoring.py` | output matches the scored contract; raw fields pass through unchanged; broken inputs are recognised as poison |
| `tests/test_kafka_flow.py` | against the real compose Kafka, on throw-away topics: N raw records give exactly N scored records with the right key and headers, the poison record lands in the DLQ, offsets are committed, a restart redoes nothing; and when Kafka refuses the outputs, nothing is committed |

The Kafka tests are **skipped** (not failed) when Kafka is not reachable on `localhost:9092`; `pytest -rs` shows why.
