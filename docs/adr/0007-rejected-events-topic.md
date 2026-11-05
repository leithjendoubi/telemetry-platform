# ADR-0007: Rejected events on telemetry.rejected.v1

- Status: accepted
- Date: 2026-11-05
- Context: week 5, step A2 (FR-ING-07 second half, AT-02). Contract: `libs/telemetry-contracts`,
  `schemas/telemetry-rejected-event.1.0.json` (step A1).

## Decisions

1. **Rejections are part of the durable write.** Accepted events (to `telemetry.raw.v1`) and rejected events (to
   `telemetry.rejected.v1`) are sent in one go, and the gateway answers 202 only when Kafka acknowledged all of them.
   FR-ING-01 ("202 only after the broker durably acknowledged the write") and AT-02 ("invalid events ... published to
   telemetry.rejected.v1") then hold for every 202.
2. **Consequence for fully rejected batches.** With Kafka down, a batch whose events are all invalid now gets 503 (it got
   202 before), because its rejections cannot be recorded. Spec 8.1's rule "a fully rejected batch gets 202 so agents do
   not retry data that can never be valid" still holds whenever Kafka works; during a broker outage 503 is the answer
   spec 8.1 lists anyway, and the retry stops at the first 202. Rejected alternative: best-effort publishing of
   rejections, which would make AT-02 untrue exactly when it matters (during incidents).
3. **A retry can repeat rejected records.** The topic is an audit trail, read by people and metrics, so duplicates are
   acceptable; `batchId` + `index` identify them. (Raw duplicates are absorbed by the analytics writer, ADR-0003.)
4. **One record per rejected event, key = `agentId`** (spec 8.3). Requests rejected as a whole (400/401/403/413/415)
   produce no record: there is no trustworthy event to report, and an unauthenticated caller must not be able to write
   into Kafka. Those cases become metrics in step B1.
5. **The record repeats the 202 entry exactly** (`index`, `eventId`, `code`, `message`) plus `agentId`, `batchId`,
   `receivedAt` and `event` = the event as sent. Messages can quote agent input (e.g. an unknown field's name), so every
   rejection message is capped at 1000 characters with a `[...]` mark, in the response and on the topic alike.
6. **`RawEventPublisher` became `EventPublisher`**, the outbound port name of spec 7, since it now writes both topics.
