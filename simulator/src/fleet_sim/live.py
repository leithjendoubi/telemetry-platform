"""Live mode (FR-SIM-01, FR-SIM-03): act like the fleet's monitoring agents and POST batches to the gateway.

One agent per service ("agent-<service>"). Every interval each agent sends ONE batch holding the samples
of its instances (10 events with the default fleet), exactly like a real agent would.

Time compression
----------------
speed = 1    real time. Event timestamps are "now".
speed = N    N times faster. The gateway rejects timestamps older than 24 h or more than 5 min in the
             future (spec 8.1), so a compressed run starts in the PAST: it begins at (now - N x duration)
             and ends at "now". That is why --duration is required when speed > 1 and N x duration
             must stay below 23 hours.

Agent behaviour on errors (spec 8.1): 429 and 503 and network errors are retried with jittered
exponential backoff and Retry-After is respected. Any other error status means the data can never be
valid, so it is counted and NOT retried.
"""

from __future__ import annotations

import json
import math
import random
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable

from .config import FleetConfig
from .model import SECONDS_PER_DAY, FleetModel, iso_ms

MAX_COMPRESSED_WINDOW_S = 23 * 3600
RETRYABLE = {429, 503}
MAX_RETRY_AFTER_S = 30.0


@dataclass
class LiveStats:
    batches_sent: int = 0
    events_sent: int = 0
    accepted: int = 0
    rejected: int = 0
    retries: int = 0
    failed_batches: int = 0
    status_counts: Counter = field(default_factory=Counter)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, **kw: int) -> None:
        with self._lock:
            for key, value in kw.items():
                setattr(self, key, getattr(self, key) + value)

    def count_status(self, status: int) -> None:
        with self._lock:
            self.status_counts[status] += 1

    def summary(self) -> str:
        statuses = ", ".join(f"{k}x{v}" for k, v in sorted(self.status_counts.items())) or "none"
        return (
            f"batches={self.batches_sent} events={self.events_sent} accepted={self.accepted} "
            f"rejected={self.rejected} retries={self.retries} failed_batches={self.failed_batches} "
            f"http_status=[{statuses}]"
        )


class LiveRunner:
    def __init__(
        self,
        model: FleetModel,
        url: str,
        speed: float = 1.0,
        duration: float | None = None,
        workers: int = 16,
        timeout: float = 10.0,
        max_retries: int = 5,
        backoff_base: float = 0.5,
        batch_seed: int | None = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
        log: Callable[[str], None] | None = None,
    ):
        if speed <= 0:
            raise ValueError("speed must be positive")
        if speed > 1 and duration is None:
            raise ValueError("--duration is required when speed > 1 (the run starts in the past)")
        if duration is not None and speed * duration > MAX_COMPRESSED_WINDOW_S:
            raise ValueError("speed x duration must stay below 23 hours (the gateway rejects older events)")
        self.model, self.cfg = model, model.config
        self.url, self.speed, self.duration = url, speed, duration
        self.workers, self.timeout = workers, timeout
        self.max_retries, self.backoff_base = max_retries, backoff_base
        self.clock, self.sleep = clock, sleep
        self.log = log or (lambda _m: None)
        self.stats = LiveStats()
        self._rng = random.Random(model.config.seed if batch_seed is None else batch_seed)
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    # ---------------------------------------------------------------- batches
    def build_batch(self, service_idx: int, tick_start_s: int) -> dict | None:
        """The batch an agent sends at the END of the interval [tick_start_s, tick_start_s + interval)."""
        cfg = self.cfg
        day = tick_start_s // SECONDS_PER_DAY
        step = (tick_start_s - day * SECONDS_PER_DAY) // cfg.interval_seconds
        sd = self.model.service_day(service_idx, day)
        events = []
        for inst in range(cfg.instances_per_service):
            event = sd.event_at(inst, step)
            if event is not None:
                events.append(event)
        if not events:
            return None
        return {
            "schemaVersion": "1.0",
            "batchId": str(uuid.UUID(int=self._rng.getrandbits(128), version=4)),
            "agent": {
                "agentId": cfg.agent_id(service_idx),
                "agentVersion": cfg.agent_version,
                "sentAt": iso_ms((tick_start_s + cfg.interval_seconds) * 1000),
            },
            "events": events,
        }

    # ---------------------------------------------------------------- sending
    def _post(self, api_key: str, body: bytes) -> tuple[int, float | None, dict | None]:
        """One HTTP attempt. Returns (status, retry_after_seconds, parsed_json_or_None). Status 0 = network error."""
        request = urllib.request.Request(
            self.url, data=body, method="POST",
            headers={"Content-Type": "application/json", "X-API-Key": api_key},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.status, None, _json_or_none(response.read())
        except urllib.error.HTTPError as err:
            retry_after = None
            raw = err.headers.get("Retry-After") if err.headers else None
            if raw:
                try:
                    retry_after = min(float(raw), MAX_RETRY_AFTER_S)
                except ValueError:
                    pass
            return err.code, retry_after, _json_or_none(err.read())
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
            return 0, None, None

    def send_batch(self, service_idx: int, batch: dict) -> None:
        body = json.dumps(batch, separators=(",", ":")).encode("utf-8")
        api_key = self.cfg.api_key(service_idx)
        attempt = 0
        while True:
            status, retry_after, payload = self._post(api_key, body)
            self.stats.count_status(status)
            if status == 202:
                self.stats.add(
                    batches_sent=1,
                    events_sent=len(batch["events"]),
                    accepted=int((payload or {}).get("accepted", 0)),
                    rejected=int((payload or {}).get("rejected", 0)),
                )
                return
            if status in RETRYABLE or status == 0:
                attempt += 1
                if attempt > self.max_retries:
                    self.stats.add(failed_batches=1)
                    return
                delay = max(retry_after or 0.0, self.backoff_base * 2 ** (attempt - 1))
                self.stats.add(retries=1)
                self.sleep(delay * self._rng.uniform(0.5, 1.5))  # jitter
                continue
            # 400, 401, 403, 413, 415, 422 ...: retrying cannot help
            self.stats.add(failed_batches=1)
            self.log(f"batch {batch['batchId']} for {batch['agent']['agentId']} refused with HTTP {status}")
            return

    # ---------------------------------------------------------------- main loop
    def run(self) -> LiveStats:
        cfg, interval = self.cfg, self.cfg.interval_seconds
        wall_start = self.clock()
        window = None if self.duration is None else self.speed * self.duration
        virtual_start = wall_start - (self.speed * self.duration if self.speed > 1 else 0.0)
        virtual_end = None if window is None else virtual_start + window
        tick_start = int(math.floor(virtual_start / interval)) * interval

        self.log(
            f"live run: {cfg.services} agents, {cfg.instances} instances, speed x{self.speed:g}, "
            f"{'until stopped' if self.duration is None else f'{self.duration:g} s'} -> {self.url}"
        )
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            while not self._stop.is_set():
                tick_end = tick_start + interval
                if virtual_end is not None and tick_end > virtual_end:
                    break
                # wait (in real time) until the virtual clock reaches the end of this interval
                wait = wall_start + (tick_end - virtual_start) / self.speed - self.clock()
                if wait > 0:
                    self.sleep(wait)
                futures = []
                for s in range(cfg.services):
                    batch = self.build_batch(s, tick_start)
                    if batch is not None:
                        futures.append(pool.submit(self.send_batch, s, batch))
                for future in futures:
                    future.result()
                tick_start = tick_end
        self.log("live run finished: " + self.stats.summary())
        return self.stats


def _json_or_none(raw: bytes) -> dict | None:
    try:
        value = json.loads(raw.decode("utf-8"))
        return value if isinstance(value, dict) else None
    except (ValueError, UnicodeDecodeError):
        return None
