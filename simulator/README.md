# Fleet simulator

Creates fake but realistic telemetry for a fleet of 35 services x 10 instances (350 instances) that report
every 15 seconds. This is the only data source of the project (spec A-04): real agents are out of scope.

**Version 0 produces NORMAL traffic only.** Faults (S1 to S7) and look-alike harmless events (B1 to B5)
come in Stage 3, when the ML work starts. Until then every row has `label = 0`.

## How the data is made (in simple words)

1. Each service gets a random "personality": how busy it is, how much CPU one request costs, how much memory it has.
2. Requests per second follow the clock: busy in the afternoon, quiet at night, lower at weekends, plus noise.
3. CPU, threads, latency and network are **calculated from the traffic**, so they move together like in a real system.
4. Memory slowly climbs and drops in a saw-tooth, like garbage collection.

Everything is derived from a **seed**. The same seed gives the same data, always. Each service and day is
generated independently, so any one day can be re-created alone.

## Commands

```bash
pip install -e ".[dev]"                        # once

python -m fleet_sim describe                   # fleet size and expected volume

# offline: write data files
python -m fleet_sim generate --out data/d1-sample --start 2026-01-01 --days 1 --format parquet
python -m fleet_sim generate --out data/quick --services 2 --days 1 --format csv      # small and fast

# live: act like the monitoring agents and POST to the gateway (needs the gateway from Stage 2)
python -m fleet_sim live --url http://localhost:8081/api/v1/telemetry/ingest            # real time, until Ctrl-C
python -m fleet_sim live --speed 60 --duration 30                                       # 30 s of real time = 30 min of data
```

Dataset **D1** (spec 9.1: 10 days, seed 42, 20,160,000 rows) is:

```bash
python -m fleet_sim generate --out data/d1 --start 2026-01-01 --days 10 --format parquet --name D1
```

Measured here with CSV output: one full day takes about 64 s and gives a 134 MB file, so D1 should take about 11 minutes.
Parquet output was not measured.

## Output

```
data/d1/
  manifest.json                      seed, config, row counts, SHA-256 hashes
  incidents.jsonl                    ground-truth incidents (empty in v0)
  date=2026-01-01/part-0000.parquet  one file per UTC day, sorted by time
```

Columns: `event_time, event_id, instance_id, service_name, environment, region, interval_seconds,`
the 12 metrics (`cpu_usage_pct, mem_used_bytes, mem_limit_bytes, net_rx_bps, net_tx_bps, disk_read_bps,
disk_write_bps, active_threads, http_rps, http_error_pct, http_p95_ms, gc_pause_ms_min`), `label`.
Metric names match the `telemetry_event` table of spec 8.4, so the bulk loader can use `COPY` directly.

**Reproducibility check.** `manifest.json` has `datasetContentSha256`, a fingerprint of the data values
(independent of CSV vs Parquet). Run `generate` twice with the same seed: the value must be identical.

## Live mode

- One agent per service, named `agent-<service>`. Every 15 s it sends one batch with its 10 instances.
- API key is `dev-agent-<service>` (development rule from `configs/default.yaml`; Stage 2 registers the same keys).
- `429`, `503` and network errors are retried with jittered exponential backoff and `Retry-After` is respected.
  Other errors (400, 401, 403, 413, 415, 422) are counted and **not** retried, because the data can never become valid.
- **Speed above 1.** The gateway rejects timestamps older than 24 h or more than 5 minutes in the future. So a
  fast run starts in the past and ends at "now": `--speed 60 --duration 30` sends 30 minutes of data that ends at the current time.
  `--duration` is therefore required when `--speed` is above 1, and `speed x duration` must stay below 23 hours.

## Tests

```bash
python -m unittest discover -s tests -v        # or: pytest
```

51 tests: configuration, determinism (including that every random stream depends on the seed), the daily
volume of 2,016,000 events, realism (daily cycle, weekend dip, correlated metrics), the contract (valid and
invalid payloads from `libs/telemetry-contracts`), offline datasets and manifest, and live mode against a
fake gateway including retry behaviour. The Parquet test is skipped unless `pyarrow` is installed.

## Not implemented yet

| Missing | Spec | Planned |
|---------|------|---------|
| Fault scenarios S1 to S7 with gradual degradation | 9.1 | Stage 3 |
| Benign confounders B1 to B5 (traffic spikes, deployments, batch jobs, late and duplicate samples) | 9.1 | Stage 3 |
| Ground-truth incident file contents (`incidents.jsonl` is created empty) | FR-SIM-04 | Stage 3 |
| Dropped samples exist (`drop_rate`); duplicated and late samples do not | 9.1 B5 | Stage 3 |
| Parallel generation for faster D1 / D1-X | 9.1 | when needed |
