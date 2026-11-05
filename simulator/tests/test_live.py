"""Live mode against a fake gateway (a tiny HTTP server inside the test)."""

import json
import threading
import time
import unittest
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import helpers

from fleet_sim.config import FleetConfig
from fleet_sim.live import LiveRunner
from fleet_sim.model import FleetModel


class FakeGateway:
    """Records every request. Can answer the first N requests (or all of them) with an error status."""

    def __init__(
        self,
        fail_first: int = 0,
        fail_status: int = 429,
        always_status: int | None = None,
        retry_after: str = "0",
    ):
        self.requests: list[dict] = []
        self.retry_after = retry_after
        self.fail_remaining = fail_first
        self.fail_status = fail_status
        self.always_status = always_status
        self.lock = threading.Lock()
        gateway = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                with gateway.lock:
                    gateway.requests.append(
                        {
                            "path": self.path,
                            "api_key": self.headers.get("X-API-Key"),
                            "content_type": self.headers.get("Content-Type"),
                            "json": json.loads(body),
                        }
                    )
                    status = gateway.always_status
                    if status is None and gateway.fail_remaining > 0:
                        gateway.fail_remaining -= 1
                        status = gateway.fail_status
                if status:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/problem+json")
                    self.send_header("Retry-After", gateway.retry_after)
                    payload = json.dumps({"title": "error", "status": status}).encode()
                else:
                    doc = json.loads(body)
                    self.send_response(202)
                    self.send_header("Content-Type", "application/json")
                    payload = json.dumps(
                        {
                            "batchId": doc["batchId"],
                            "accepted": len(doc["events"]),
                            "rejected": 0,
                            "rejections": [],
                        }
                    ).encode()
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):  # keep test output quiet
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/api/v1/telemetry/ingest"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()


CFG = FleetConfig(services=3, instances_per_service=10)


def make_runner(url, **kw):
    kw.setdefault("speed", 150.0)  # 150 virtual seconds = 10 ticks of 15 s ...
    kw.setdefault("duration", 1.0)  # ... squeezed into 1 real second
    kw.setdefault("backoff_base", 0.001)
    kw.setdefault("workers", 4)
    return LiveRunner(FleetModel(CFG), url=url, **kw)


class LiveModeTests(unittest.TestCase):
    def test_sends_valid_batches_with_the_right_headers(self):
        validator = helpers.make_validator()
        started = datetime.now(UTC)
        with FakeGateway() as gw:
            stats = make_runner(gw.url).run()
        finished = datetime.now(UTC)

        self.assertEqual(stats.batches_sent, 30)  # 3 agents x 10 ticks
        self.assertEqual(stats.events_sent, 300)  # 30 events per tick
        self.assertEqual(stats.accepted, 300)
        self.assertEqual(stats.failed_batches, 0)
        self.assertEqual(stats.retries, 0)
        self.assertEqual(len(gw.requests), 30)

        batch_ids, event_ids = set(), set()
        for req in gw.requests:
            doc = req["json"]
            self.assertEqual(req["path"], "/api/v1/telemetry/ingest")
            self.assertEqual(req["content_type"], "application/json")
            self.assertEqual(req["api_key"], f"dev-{doc['agent']['agentId']}")
            self.assertEqual(list(validator.iter_errors(doc)), [])
            self.assertLessEqual(len(doc["events"]), 100)
            batch_ids.add(doc["batchId"])
            for ev in doc["events"]:
                event_ids.add(ev["eventId"])
                self.assertEqual(
                    helpers.semantic_errors(ev, finished), []
                )  # not older than 24 h, not in the future
                self.assertLessEqual(helpers.parse_ts(ev["timestamp"]), finished + timedelta(seconds=1))
        self.assertEqual(len(batch_ids), 30)
        self.assertEqual(len(event_ids), 300)
        self.assertGreater(started, finished - timedelta(seconds=30))

    def test_compressed_run_ends_at_now_not_in_the_future(self):
        with FakeGateway() as gw:
            runner = make_runner(gw.url)
            before = time.time()
            runner.run()
        newest = max(
            helpers.parse_ts(e["timestamp"]).timestamp() for r in gw.requests for e in r["json"]["events"]
        )
        oldest = min(
            helpers.parse_ts(e["timestamp"]).timestamp() for r in gw.requests for e in r["json"]["events"]
        )
        self.assertLessEqual(newest, time.time())
        self.assertLess(oldest, before - 100)  # a 150 s virtual window really starts in the past

    def test_retries_429_and_503_then_succeeds(self):
        for status in (429, 503):
            with self.subTest(status=status), FakeGateway(fail_first=3, fail_status=status) as gw:
                stats = make_runner(gw.url).run()
                self.assertEqual(stats.retries, 3)
                self.assertEqual(stats.status_counts[status], 3)
                self.assertEqual(stats.batches_sent, 30)  # nothing lost
                self.assertEqual(stats.failed_batches, 0)
                self.assertEqual(len(gw.requests), 33)  # 30 batches + 3 retried attempts

    def test_retry_after_is_a_minimum_jitter_never_shortens_it(self):
        waits: list[float] = []

        def sleep(seconds: float) -> None:
            waits.append(seconds)
            if seconds < 1:  # the runner's own pacing really waits; the long retry waits are only recorded
                time.sleep(seconds)

        # one worker: the 4 refusals hit the first batch in a fixed order (with several workers, which
        # batch gets them depends on timing, and one batch could run out of retries)
        with FakeGateway(fail_first=4, fail_status=429, retry_after="7") as gw:
            stats = make_runner(gw.url, sleep=sleep, workers=1).run()
        retry_waits = [w for w in waits if w >= 1]
        self.assertEqual(stats.retries, 4)
        self.assertEqual(len(retry_waits), 4)
        self.assertTrue(all(w >= 7 for w in retry_waits), retry_waits)

    def test_does_not_retry_permanent_errors(self):
        with FakeGateway(always_status=401) as gw:
            stats = make_runner(gw.url).run()
        self.assertEqual(stats.retries, 0)
        self.assertEqual(stats.failed_batches, 30)
        self.assertEqual(stats.batches_sent, 0)
        self.assertEqual(len(gw.requests), 30)  # exactly one attempt per batch

    def test_gives_up_after_max_retries(self):
        with FakeGateway(always_status=503) as gw:
            stats = make_runner(gw.url, max_retries=2).run()
        self.assertEqual(stats.failed_batches, 30)
        self.assertEqual(stats.retries, 60)  # 2 retries per batch
        self.assertEqual(len(gw.requests), 90)  # 3 attempts per batch

    def test_unreachable_gateway_is_survived(self):
        # port 9 (discard) on localhost refuses the connection: network error, retried, then counted failed
        stats = make_runner(
            "http://127.0.0.1:9/api/v1/telemetry/ingest", max_retries=1, duration=0.3, speed=50.0
        ).run()
        self.assertEqual(stats.batches_sent, 0)
        self.assertGreater(stats.failed_batches, 0)

    def test_speed_above_one_requires_a_duration(self):
        with self.assertRaises(ValueError):
            LiveRunner(FleetModel(CFG), url="http://x.invalid", speed=10.0)

    def test_compressed_window_cannot_exceed_23_hours(self):
        with self.assertRaises(ValueError):
            LiveRunner(FleetModel(CFG), url="http://x.invalid", speed=1000.0, duration=100.0)

    def test_only_http_urls_are_accepted(self):
        for url in ("file:///etc/passwd", "ftp://x.invalid/ingest", "localhost:8081/api"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                LiveRunner(FleetModel(CFG), url=url)
        LiveRunner(FleetModel(CFG), url="https://x.invalid/api/v1/telemetry/ingest")  # no error


if __name__ == "__main__":
    unittest.main()
