# ADR-0002: Detection pass-through stub and the scored record

- Status: accepted
- Date: 2026-10-14
- Context: Stage 2, step 2 (spec 16.2 week 4, gate G1)

## Decisions

1. **Scored record = raw record + `prediction` object + `scoredAt`**, as spec 8.3 and FR-DET-03 describe
   (`prediction = {probability, isAnomaly, threshold, modelVersion, featureSetVersion, warmup}`).
   The first CLAUDE.md plan said flat fields `anomalyProb`, `isAnomaly`, `modelVersion`; the spec wins.
   Defined in `libs/telemetry-contracts/schemas/telemetry-scored-event.1.0.json`; a contract test keeps the raw
   fields identical to the raw schema. The analytics writer maps `prediction.probability` to `anomaly_prob`,
   `prediction.isAnomaly` to `is_anomaly`, `prediction.warmup` to `is_warmup`, `prediction.modelVersion` to `model_version`.
2. **Port 8081 for the gateway** everywhere (config, simulator default, compose later), because 8080 is used by a
   Pi-hole on the development machine.
3. **At-least-once loop:** consume a batch, produce, flush, then commit synchronously. A refused output stops the
   process without committing (exit code 1); the container restart policy (step 4) brings it back.
4. **Poison records go to the DLQ at once** (attempt count 1): a record that is not JSON or breaks the raw contract
   cannot become valid by retrying. Retries with a count > 1 will matter for transient errors in the real service.
5. **Incoming records are checked against the raw JSON Schema.** It costs little at the skeleton's rate (about 23
   events/s for the default fleet; 84,000 records were processed in well under a minute) and catches contract drift early.
   To be re-measured in the week-11 latency profiling.
6. **Integration tests use the compose Kafka with throw-away topics** instead of Testcontainers for Python: no new
   dependency, same broker image as production. The tests are skipped, not failed, when Kafka is down.
