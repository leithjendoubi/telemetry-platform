# ADR-0012: gzip request bodies and graceful shutdown

- Status: accepted. Designed by Claude within FR-ING-08 and FR-ING-11; no choice was put to Leith in this step.
- Date: 2026-11-05
- Context: week 5, step A7.

## Decisions

1. **gzip (FR-ING-08).** `Content-Encoding: gzip` is accepted; `identity` or no header means plain; anything else
   (e.g. `br`, `deflate`) stays 415. The 256 KiB limit applies to the compressed body while reading and again
   **after decompression** (spec 8.1). Decompression stops as soon as the output passes the limit, so a "zip bomb"
   (10 MiB of zeros in about 10 KiB) costs at most 256 KiB of memory and gets 413. A body that is not valid gzip
   gets 400. Decompression works on bytes already in memory, so it does not block the event loop (BlockHound: 0).
   The simulator keeps sending plain JSON.
2. **Graceful shutdown (FR-ING-11)** was already configured (`server.shutdown: graceful`, 30 s per shutdown phase,
   compose `stop_grace_period: 35s`) but never tested. `GracefulShutdownIT` starts its own gateway, holds a request in
   progress, closes the gateway, checks that new connections are refused, and that the held request still gets 202
   and its event is in Kafka. The held body is sent 6 s after the shutdown starts: Reactor Netty lets connections
   finish for a few seconds even with `server.shutdown: immediate`, so a quicker test could not tell the two apart
   (a planted `immediate` passed the first version). The Kafka producer is closed after the web server has stopped,
   so its last records are flushed; since 202 is only sent after Kafka acknowledged, no answered event can be lost.
