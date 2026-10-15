# ADR-0001: Ingestion gateway skeleton choices

- Status: accepted
- Date: 2026-10-14
- Context: Stage 2, step 1 (spec 16.2 week 4, gate G1)

## Context

The walking skeleton needs a working gateway now; the hardening of spec FR-ING-04 to FR-ING-10 is planned for week 5.
Several small choices were not fixed by the spec.

## Decisions

1. **Kafka record of `telemetry.raw.v1` is flat**: the accepted event unchanged, plus `receivedAt`, `agentId`,
   `batchId` at top level (spec 8.3 "accepted event plus receivedAt, agentId, batchId"). Defined first in
   `libs/telemetry-contracts/schemas/telemetry-raw-event.1.0.json`; a Python test checks its event fields stay
   identical to the batch schema. Spec 8.3 headers are set, except `traceparent` (week 5, OpenTelemetry).
2. **One schema file, read from the contracts library at build time.** Maven copies `libs/telemetry-contracts`
   into the jar and the test classpath; there is no hand-made copy. Consequence: the Docker build context for
   the gateway must be the repository root (step 4).
3. **Envelope vs event errors.** A schema error located inside `events[i]` rejects only that event
   (`SCHEMA_VIOLATION`, 202). Any other schema error, including `events` itself being empty, rejects the batch with 400.
   A batch with more than 100 events is 413, as spec 8.1's error table says, not 400.
4. **One code per rejected event**, the first rule broken, in this order: schema, allowed service, timestamp
   window, memory, duplicate. For duplicates, the first *accepted* copy wins, so a batch never publishes the same
   eventId twice.
5. **Agents from configuration** (`dev-agents.yml`, SHA-256 hashes only) until the `ingestion_db` registry of week 5.
   `serviceName` allow-lists and the agentId/key match (403) are already enforced because they cost almost nothing.
6. **Short producer timeouts** (`max.block.ms` 5 s, `delivery.timeout.ms` 10 s) so a Kafka outage becomes a fast 503
   that agents retry (spec 6.8), instead of requests hanging for the 2-minute client default. If any record of a batch
   fails, the whole batch gets 503; the agent resends it and the analytics writer's `ON CONFLICT DO NOTHING`
   absorbs the duplicates (at-least-once, ADR-007 of the spec).
7. **Integration tests use Testcontainers** with the same `apache/kafka:3.8.1` image as compose (spec 6.5 lists
   Testcontainers). Version pinned to 1.21.4 because 1.21.3 fails against Docker Engine 29.

## Consequences

Week 5 adds behaviour (registry, rate limits, rejected topic, gzip, BlockHound) without changing the HTTP contract
or the Kafka record shape.
