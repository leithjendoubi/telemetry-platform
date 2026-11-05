# ADR-0009: Rate limits and the in-flight cap

- Status: accepted. Designed by Claude within FR-ING-06 and spec 6.8; no choice was put to Leith in this step.
- Date: 2026-11-05
- Context: week 5, step A4 (FR-ING-06, NFR-REL-04, AT-04).

## Decisions

1. **One token bucket per agent** (Bucket4j 8.21.0, spec 6.5; 8.22.0 was only 2 days old). Capacity = `burst`, refill
   = `rate_limit_rps` per second, both from table `agent` (defaults 50 and 100, spec 8.4). An empty bucket gives **429**
   with `Retry-After` = seconds until the next token, rounded **up** (at least 1), so an agent is never invited back too
   early. Buckets are keyed by agent and its limits, so a changed limit applies at the next registry refresh (10 s).
2. **Buckets live in the memory of each gateway instance** (Caffeine: idle buckets dropped after 10 min, at most 10,000).
   With N gateway replicas an agent could send N times its limit. Acceptable for one replica now; a shared store
   (Bucket4j has Redis/JDBC back-ends) is needed before scaling out (NFR-SCA-01).
3. **Global in-flight cap of 2,000 requests per instance** (spec 6.8): one more gets **503** with `Retry-After`, never a
   queue (principle 6). The slot is freed however the request ends (answer, error, client hang-up).
4. **Order of checks:** in-flight cap, then API key, then the agent's bucket, then content. The cap comes first because
   it protects everything behind it, including the database lookup.
5. **The overload 503 does not wait for the body** (unlike the other early answers, ADR-0008): under overload, reading
   up to 256 KiB per refused request would add load. A client that loses this answer sees a network error, which
   agents retry exactly like a 503 (spec 8.1). The 429 does wait for the body, so the agent reliably sees Retry-After.
6. **The simulator treats `Retry-After` as a minimum.** Its jitter used to multiply the wait by 0.5-1.5, so it could come
   back after half the time the gateway asked for; now the jitter spreads only the backoff.

## Not done here

Metrics for rate-limited requests and in-flight count (spec 12.2) come with step B1; `InFlightLimiter.inFlight()` is
ready for it.
