# ADR-0008: Agent registry in ingestion_db

- Status: accepted (decisions 1-3 by Leith, 2026-11-05)
- Date: 2026-11-05
- Context: week 5, step A3 (FR-ING-05, AT-03). Replaces the configuration file `dev-agents.yml` of ADR-0001.

## Decisions

1. **Table `agent` in `ingestion_db`, exactly the columns of spec 8.4**, created by Flyway. The database itself refuses
   bad rows (hash not 64 lower-case hex, empty service list, unknown status, non-positive limits), so a mistake in a seed
   or a future admin API cannot slip through.
2. **Flyway over JDBC at start-up only, R2DBC at runtime.** R2DBC (spec 6.5) has no migration tool; Flyway is what the
   analytics service already uses. The JDBC connection is opened once by Flyway and never by request handling, so the
   event loop stays non-blocking (FR-ING-02). Both connections use the gateway's own role `ingestion` (rule 2).
3. **Cache of 10 s in front of the database** (`AgentRegistry`, Caffeine, version managed by Spring Boot). A revocation
   takes effect within 10 s. The cache is bounded (10,000 hashes) and also keeps "no such agent", so a flood of random
   keys neither reaches the database per request nor fills the memory. Concurrent requests with one key share one
   lookup; a client hanging up does not cancel it for the others. Failed lookups are not cached.
4. **Answers follow spec 8.1:** missing or unknown key 401, revoked key 403, agent-ID mismatch 403 (unchanged).
   A database that does not answer within 2 s gives **503 with Retry-After**, never 401 (that would tell a valid agent
   its key is wrong) and never a pass.
5. **The 35 simulator agents are a development seed** (`db/dev-seed/R__dev_agents.sql`, generated from the old
   `dev-agents.yml`, hashes checked), loaded only because the default Flyway locations include it. A production
   deployment sets `GATEWAY_DB_LOCATIONS=classpath:db/migration`. The seed never overwrites an existing agent
   (`ON CONFLICT DO NOTHING`), so a revoked dev agent stays revoked. The Gitleaks exception now covers this file too.
6. **Early answers wait for the body.** Found in the live check of 2026-11-05: answers decided from the headers alone
   (401, 403, 415, registry 503) were sent before the body arrived; with `Connection: close` (Python's urllib, i.e. the
   simulator) the gateway then closed while the client was still sending, and the client lost the answer (WinError
   10053; 15-103 of 200 requests in a probe). A revoked agent saw "network error" and retried instead of stopping.
   Such answers now read and drop the rest of the body first, bounded like a normal read (256 KiB, 5 s), so an
   unauthenticated client cannot hold the gateway longer than an authenticated one. A socket test that pauses 500 ms
   between headers and body fails without this and passes with it. (The bug existed before this step for 401; it
   showed only now because a revoked key produces such answers in normal traffic.)
7. **Packages:** `auth` keeps the pure rules (hashing, the 401/403/pass decision) without Spring or Reactor; the new
   `registry` package is the database adapter and must not know HTTP or Kafka (ArchUnit).

## Not done (spec FR-ING-10, priority "Could")

No admin API for registration, key rotation and revocation; agents are managed with SQL for now.
