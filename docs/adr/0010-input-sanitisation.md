# ADR-0010: Input sanitisation (FR-ING-04)

- Status: accepted. Designed by Claude within FR-ING-04; no choice was put to Leith in this step.
- Date: 2026-11-05
- Context: week 5, step A5. FR-ING-04: "allow-list patterns for identifiers, reject control characters, NaN/Infinity
  and unknown fields, normalise timestamps to UTC". A check of what was already covered found three gaps.

## What was already covered

Identifier allow-list patterns, unknown fields at every level (`additionalProperties: false`), the literals `NaN` /
`Infinity` (refused by the JSON parser), and control characters inside identifiers. New contract cases now prove each
of these (`unknown-event-field`, `unknown-resource-field`, `identifier-with-nul`).

## Gaps found and fixed

1. **A trailing newline passed the identifier pattern.** JSON Schema patterns are ECMA-262, where `$` is the very end
   of the text; Java's and Python's regex engines also let `$` match before a final newline, so
   `"checkout-api-07\n"` was accepted by the gateway and by the Python contract tests (measured, both). The schema is
   right; the validators read it too loosely. Fix: an unescaped `$` outside `[...]` is read as `\z` / `\Z`
   (`EcmaRegularExpressionFactory` in the gateway, `ecma_anchors` in the Python tests). Rejected alternatives: the
   Joni or GraalJS regex engines of networknt (extra dependency, and Python would still need its own fix); changing
   the schema (`telemetry-batch.1.0.json` must not change in a breaking way, and it is already correct).
2. **A number too large for 64 bits became `Infinity`.** `1e400` is valid JSON and passes `minimum: 0`, but every
   parser turns it into Infinity; Jackson then wrote it to Kafka as the text `"Infinity"`, breaking the raw contract
   for every consumer. Fix: a rule after the schema rejects such an event with `SEMANTIC_VIOLATION` (contract README
   table; the Python tests mirror it). Contract case `metric-overflows-to-infinity` uses a new marker `NUMBER:<literal>`,
   because the test tools would otherwise rewrite the number before it reaches the validator.
3. **Timestamps with an offset were passed on unchanged.** Accepted events now carry their timestamp in UTC: the same
   instant with `Z` and the same number of fraction digits. A timestamp already in UTC is kept byte for byte, so normal
   traffic is unchanged. The 202 body and `telemetry.rejected.v1` keep what the agent sent.

## Tests

Six new contract cases (both languages), UTC unit tests, an integration test from HTTP to Kafka for the UTC rule.
Planted bugs (ECMA adapter not used, finite rule removed, UTC step removed) are each caught.
