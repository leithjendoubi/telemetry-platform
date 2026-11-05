"""End-to-end smoke test of the walking skeleton (spec 16.2 week 4, gate G1).

    simulator -> gateway -> telemetry.raw.v1 -> detection stub -> telemetry.scored.v1
              -> analytics -> TimescaleDB -> query API

It starts the compose stack, sends traffic with the simulator, then reconciles every hop:
the number of events the gateway accepted must be the number of records on both topics, the number of rows in
telemetry_event (with no duplicates), and the query endpoint must return the stored points.

Run it with the simulator's Python, from the repository root (PowerShell):

    simulator\\.venv\\Scripts\\python bench\\scripts\\smoke_test.py --clean

--clean first runs `docker compose down -v`, which DELETES all Kafka and TimescaleDB data of this stack, so
the counts start from zero. Without --clean the stack must already be empty, or the test refuses to run:
the simulator is deterministic, so leftovers from earlier runs would make the counts meaningless.

Exit code 0 = every check passed.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = [
    "docker",
    "compose",
    "-f",
    str(ROOT / "infra/compose/docker-compose.yml"),
    "--profile",
    "core",
    "--env-file",
    str(ROOT / ".env"),
]
KAFKA = "telemetry-kafka"
DB = "telemetry-timescaledb"
KAFKA_BIN = "/opt/kafka/bin"
GATEWAY = "http://localhost:8081"
ANALYTICS = "http://localhost:8082"


# ---------------------------------------------------------------- small helpers


def run(cmd: list[str], **kw) -> str:
    # commands are fixed lists built in this file (docker, the simulator); no shell, no outside input
    result = subprocess.run(cmd, capture_output=True, text=True, **kw)  # noqa: S603
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(cmd)}\n{result.stdout}\n{result.stderr}"
        )
    return result.stdout


def sql(query: str) -> str:
    return run(["docker", "exec", DB, "psql", "-U", "postgres", "-d", "analytics_db", "-tAc", query]).strip()


def topic_records(topic: str) -> int:
    """Records ever written to a topic = sum of the end offsets of its partitions."""
    out = run(
        [
            "docker",
            "exec",
            KAFKA,
            f"{KAFKA_BIN}/kafka-get-offsets.sh",
            "--bootstrap-server",
            "localhost:19092",
            "--topic",
            topic,
        ]
    )
    return sum(int(line.rsplit(":", 1)[1]) for line in out.splitlines() if line.count(":") >= 2)


def group_lag(group: str) -> int | None:
    """Total lag of a consumer group, or None if the group has not committed anything yet."""
    out = run(
        [
            "docker",
            "exec",
            KAFKA,
            f"{KAFKA_BIN}/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "localhost:19092",
            "--describe",
            "--group",
            group,
        ]
    )
    rows = [line.split() for line in out.splitlines() if line.strip()]
    header = next((row for row in rows if "LAG" in row), None)
    if header is None:
        return None
    i = header.index("LAG")
    lags = [row[i] for row in rows if row is not header and len(row) > i]
    if not lags or all(x == "-" for x in lags):
        return None
    return sum(int(x) for x in lags if x != "-")


def http_json(url: str) -> dict:
    if not url.startswith("http://localhost:"):
        raise ValueError(f"unexpected URL {url}")
    with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310 - checked just above
        return json.loads(response.read())


def prometheus_value(url: str, series: str) -> float | None:
    """Value of one series (name plus labels, as written in the exposition text) at a /metrics URL."""
    if not url.startswith("http://localhost:"):
        raise ValueError(f"unexpected URL {url}")
    with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310 - checked just above
        for line in response.read().decode("utf-8").splitlines():
            if line.startswith(series + " "):
                return float(line.rsplit(" ", 1)[1])
    return None


def container_health(name: str) -> str:
    return run(["docker", "inspect", "-f", "{{.State.Health.Status}}", name]).strip()


def log(message: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)


class Checks:
    def __init__(self):
        self.rows: list[tuple[str, object, object, bool]] = []

    def equal(self, name: str, expected, actual) -> None:
        self.rows.append((name, expected, actual, expected == actual))

    def true(self, name: str, ok: bool, detail: object = "") -> None:
        self.rows.append((name, "yes", detail if detail != "" else ("yes" if ok else "no"), ok))

    def report(self) -> bool:
        width = max(len(r[0]) for r in self.rows)
        print()
        print(f"{'check'.ljust(width)}  {'expected':>10}  {'actual':>10}  result")
        print("-" * (width + 36))
        for name, expected, actual, ok in self.rows:
            print(f"{name.ljust(width)}  {expected!s:>10}  {actual!s:>10}  {'PASS' if ok else 'FAIL'}")
        passed = all(r[3] for r in self.rows)
        print("-" * (width + 36))
        verdict = "PASSED" if passed else "FAILED"
        print(f"SMOKE TEST {verdict}: {sum(r[3] for r in self.rows)}/{len(self.rows)} checks")
        return passed


# ---------------------------------------------------------------- the test


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--clean", action="store_true", help="docker compose down -v first (DELETES the stack's data)"
    )
    parser.add_argument("--no-build", action="store_true", help="do not rebuild the service images")
    parser.add_argument("--speed", type=float, default=60)
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument(
        "--instances", type=int, default=350, help="instances of the simulator config (default fleet)"
    )
    parser.add_argument("--services", type=int, default=35)
    parser.add_argument(
        "--timeout", type=float, default=180, help="seconds to wait for the pipeline to drain"
    )
    args = parser.parse_args()

    # 1. stack up, from a clean state
    if args.clean:
        log("docker compose down -v  (deleting this stack's Kafka and TimescaleDB volumes)")
        run([*COMPOSE, "down", "-v"])
    log("docker compose up -d --wait" + ("" if args.no_build else " --build"))
    run([*COMPOSE, "up", "-d", "--wait", *([] if args.no_build else ["--build"])], timeout=900)

    existing_rows = int(sql("SELECT count(*) FROM telemetry_event"))
    existing_raw = topic_records("telemetry.raw.v1")
    if existing_rows or existing_raw:
        log(f"not a clean start: {existing_rows} rows, {existing_raw} raw records present. Use --clean.")
        return 2
    log("stack is up and empty")

    # 2. traffic
    log(f"simulator: live --speed {args.speed:g} --duration {args.duration:g}")
    wall_start = datetime.now(UTC)
    env = dict(os.environ, PYTHONPATH=str(ROOT / "simulator" / "src"))
    sim_out = run(
        [
            sys.executable,
            "-m",
            "fleet_sim",
            "live",
            "--speed",
            str(args.speed),
            "--duration",
            str(args.duration),
            "--url",
            f"{GATEWAY}/api/v1/telemetry/ingest",
        ],
        cwd=ROOT / "simulator",
        env=env,
        timeout=args.duration + 120,
    )
    summary = sim_out.strip().splitlines()[-1]
    log(summary)
    numbers = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", summary)}
    accepted = numbers["accepted"]

    # 3. wait until the last hop has caught up (rows stop changing and equal accepted, or timeout)
    log(f"waiting for the pipeline to drain (up to {args.timeout:g} s)")
    deadline = time.monotonic() + args.timeout
    rows, previous = -1, -2
    while time.monotonic() < deadline:
        rows = int(sql("SELECT count(*) FROM telemetry_event"))
        if rows == accepted and rows == previous:
            break
        previous = rows
        time.sleep(3)
    log(f"telemetry_event rows: {rows}")

    # 4. reconcile every hop
    c = Checks()
    c.equal("simulator: accepted = sent", numbers["events"], accepted)
    c.equal("simulator: rejected", 0, numbers["rejected"])
    c.equal("simulator: failed batches", 0, numbers["failed_batches"])
    c.equal("kafka telemetry.raw.v1 records", accepted, topic_records("telemetry.raw.v1"))
    c.equal("kafka telemetry.scored.v1 records", accepted, topic_records("telemetry.scored.v1"))
    c.equal("kafka telemetry.raw.v1.dlq records", 0, topic_records("telemetry.raw.v1.dlq"))
    c.equal("kafka telemetry.scored.v1.dlq records", 0, topic_records("telemetry.scored.v1.dlq"))
    c.equal("lag of group detection", 0, group_lag("detection"))
    c.equal("lag of group analytics-writer", 0, group_lag("analytics-writer"))
    c.equal("db rows = accepted", accepted, rows)
    c.equal("db distinct event_id", rows, int(sql("SELECT count(DISTINCT event_id) FROM telemetry_event")))
    c.equal(
        "db instances", args.instances, int(sql("SELECT count(DISTINCT instance_id) FROM telemetry_event"))
    )
    c.equal(
        "db services", args.services, int(sql("SELECT count(DISTINCT service_name) FROM telemetry_event"))
    )
    c.equal(
        "db rows with model stub-0",
        rows,
        int(sql("SELECT count(*) FROM telemetry_event WHERE model_version = 'stub-0' AND NOT is_anomaly")),
    )
    c.equal(
        "db rows with received_at <= scored_at",
        rows,
        int(sql("SELECT count(*) FROM telemetry_event WHERE received_at <= scored_at")),
    )

    # 5. read one instance back over HTTP: same points as in the database
    instance = sql("SELECT instance_id FROM telemetry_event ORDER BY instance_id LIMIT 1")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", instance):  # contract identifier pattern
        raise ValueError(f"unexpected instance id {instance!r}")
    db_points = int(sql(f"SELECT count(*) FROM telemetry_event WHERE instance_id = '{instance}'"))  # noqa: S608
    window_from = (
        wall_start - timedelta(seconds=args.speed * args.duration) - timedelta(minutes=1)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    window_to = (datetime.now(UTC) + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    series = http_json(
        f"{ANALYTICS}/api/v1/analytics/instances/{instance}/series"
        f"?metric=cpuUsagePercent&from={window_from}&to={window_to}"
    )
    times = [p["time"] for p in series["points"]]
    c.equal(f"api points of {instance}", db_points, len(series["points"]))
    c.true("api points in time order", times == sorted(times) and len(times) > 0)
    c.equal("api truncated", False, series["truncated"])

    # 6. health of the services
    c.equal("gateway /health/ready", "UP", http_json(f"{GATEWAY}/health/ready")["status"])
    c.equal("analytics /health/ready", "UP", http_json(f"{ANALYTICS}/health/ready")["status"])
    c.equal("detection container health", "healthy", container_health("telemetry-detection-service"))

    # 7. metrics (FR-ING-09): the gateway counted exactly what it accepted, in the real container
    c.equal(
        "gateway /metrics events accepted",
        float(accepted),
        prometheus_value(f"{GATEWAY}/metrics", 'gateway_events_accepted_total{service="ingestion-gateway"}'),
    )

    return 0 if c.report() else 1


if __name__ == "__main__":
    sys.exit(main())
