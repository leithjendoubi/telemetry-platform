"""Fleet model: deterministic generation of NORMAL telemetry (v0).

Design in one paragraph
-----------------------
Every number is a pure function of (seed, service, day). Nothing depends on the order in which
services or days are generated, so a day can be regenerated alone, in parallel, or in live mode and
gives bit-identical values. Each service has a random "profile" (typical traffic, CPU cost per
request, memory size, ...). From the profile, requests per second follow a daily cycle (busy in the
afternoon, quiet at night, lower at weekends) with smooth and fast noise. CPU, threads, latency,
network and GC are then DERIVED from the load, so the metrics are correlated like in real systems
(more traffic -> more CPU -> more threads -> slower responses). Memory follows a saw-tooth that
mimics garbage-collection cycles.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone

import numpy as np

from .config import SERVICE_NAMES, FleetConfig

SECONDS_PER_DAY = 86_400

# (name in the JSON payload, name in the database / dataset). Order is the column order.
METRICS: tuple[tuple[str, str], ...] = (
    ("cpuUsagePercent", "cpu_usage_pct"),
    ("memoryUsedBytes", "mem_used_bytes"),
    ("memoryLimitBytes", "mem_limit_bytes"),
    ("networkRxBytesPerSec", "net_rx_bps"),
    ("networkTxBytesPerSec", "net_tx_bps"),
    ("diskReadBytesPerSec", "disk_read_bps"),
    ("diskWriteBytesPerSec", "disk_write_bps"),
    ("activeThreadCount", "active_threads"),
    ("httpRequestsPerSec", "http_rps"),
    ("httpErrorRatePercent", "http_error_pct"),
    ("httpLatencyP95Ms", "http_p95_ms"),
    ("gcPauseMsPerMin", "gc_pause_ms_min"),
)
INTEGER_METRICS = {"memoryUsedBytes", "memoryLimitBytes", "activeThreadCount"}

# Salts keep the random streams of different purposes independent of each other.
_SALT_PROFILE, _SALT_INSTANCE, _SALT_DAY, _SALT_DROP, _SALT_IDS = 1, 2, 10, 11, 12


@dataclass(frozen=True)
class ServiceProfile:
    """Typical behaviour of one service. All instances of the service share it (with small variations)."""

    index: int
    name: str
    region: str
    base_rps: float          # mean requests per second per instance
    cpu_base: float          # CPU % at zero traffic
    target_cpu: float        # CPU % at mean traffic
    threads_base: float
    threads_extra: float     # extra threads at mean traffic
    mem_limit_bytes: int
    mem_base_ratio: float
    mem_saw_amp: float       # height of the GC saw-tooth (fraction of the limit)
    mem_saw_period: float    # saw-tooth period in samples
    lat_base_ms: float
    err_base_pct: float
    gc_base_ms: float
    rx_per_req: float        # bytes received per request
    tx_per_req: float
    disk_read_base: float
    disk_write_base: float
    peak_hour: float         # UTC hour of the daily traffic peak
    diurnal_amp: float       # 0.3 = traffic swings +-30% around the mean over the day
    weekend_factor: float
    instance_factor: np.ndarray    # per-instance traffic share (instances are not identical)
    instance_offset_ms: np.ndarray  # per-instance sampling offset inside the interval
    instance_phase: np.ndarray      # per-instance saw-tooth phase (samples)


@dataclass
class ServiceDay:
    """All samples of one service on one UTC day. Arrays have shape (instances, steps_per_day)."""

    profile: ServiceProfile
    config: FleetConfig
    day_number: int           # days since 1970-01-01 (UTC)
    ts_ms: np.ndarray         # int64 sampling time in ms since epoch
    metrics: dict[str, np.ndarray]   # keyed by JSON name
    keep: np.ndarray          # bool: False = sample dropped (never sent)
    _ids: tuple[np.ndarray, np.ndarray] | None = field(default=None, repr=False)  # lazy cache

    @property
    def n_instances(self) -> int:
        return self.ts_ms.shape[0]

    @property
    def steps(self) -> int:
        return self.ts_ms.shape[1]

    def instance_id(self, i: int) -> str:
        return f"{self.profile.name}-{i + 1:02d}"

    # ---------------------------------------------------------------- event ids
    def _id_parts(self) -> tuple[np.ndarray, np.ndarray]:
        """Random bits for every sample of the day, generated once and cached (live mode asks per event)."""
        if self._ids is None:
            rng = np.random.default_rng(
                np.random.SeedSequence([self.config.seed, _SALT_IDS, self.profile.index, self.day_number])
            )
            shape = self.ts_ms.shape
            a = rng.integers(0, 1 << 12, size=shape, dtype=np.int64)
            b = rng.integers(0, 1 << 62, size=shape, dtype=np.int64)
            self._ids = (a, b)
        return self._ids

    def event_ids_for_mask(self, mask: np.ndarray) -> list[str]:
        """UUIDv7-style ids (48-bit ms timestamp + random bits), deterministic. Row-major order."""
        a, b = self._id_parts()
        return _uuid7_list(self.ts_ms[mask], a[mask], b[mask])

    # ---------------------------------------------------------------- bulk rows (offline mode)
    def rows(self) -> dict[str, np.ndarray]:
        """Kept samples as flat columns, instance-major order. Column names follow the database table."""
        mask = self.keep
        count = int(mask.sum())
        names = np.array([self.instance_id(i) for i in range(self.n_instances)], dtype=object)
        cols: dict[str, np.ndarray] = {
            "event_time_ms": self.ts_ms[mask],
            "event_id": np.array(self.event_ids_for_mask(mask), dtype=object),
            "instance_id": np.repeat(names, self.steps)[mask.ravel()],
            "service_name": np.full(count, self.profile.name, dtype=object),
            "environment": np.full(count, self.config.environment, dtype=object),
            "region": np.full(count, self.profile.region, dtype=object),
            "interval_seconds": np.full(count, self.config.interval_seconds, dtype=np.int16),
        }
        for api_name, column in METRICS:
            cols[column] = self.metrics[api_name][mask]
        cols["label"] = np.zeros(count, dtype=np.int8)  # v0: no faults, every sample is normal
        return cols

    # ---------------------------------------------------------------- single event (live mode)
    def event_at(self, inst: int, step: int, event_id: str | None = None) -> dict | None:
        """One event in the JSON shape of the ingest contract, or None if this sample was dropped."""
        if not self.keep[inst, step]:
            return None
        ts = int(self.ts_ms[inst, step])
        if event_id is None:
            a, b = self._id_parts()
            event_id = _uuid7(ts, int(a[inst, step]), int(b[inst, step]))
        metrics: dict[str, float | int] = {}
        for api_name, _ in METRICS:
            value = self.metrics[api_name][inst, step]
            metrics[api_name] = int(value) if api_name in INTEGER_METRICS else float(value)
        return {
            "eventId": event_id,
            "timestamp": iso_ms(ts),
            "intervalSeconds": self.config.interval_seconds,
            "resource": {
                "serviceName": self.profile.name,
                "instanceId": self.instance_id(inst),
                "environment": self.config.environment,
                "region": self.profile.region,
            },
            "metrics": metrics,
        }


class FleetModel:
    """Builds the 35 service profiles once, then generates any (service, day) on demand."""

    def __init__(self, config: FleetConfig):
        self.config = config
        self.profiles: list[ServiceProfile] = [self._make_profile(i) for i in range(config.services)]
        self._cache: dict[tuple[int, int], ServiceDay] = {}

    # ---------------------------------------------------------------- profiles
    def _make_profile(self, idx: int) -> ServiceProfile:
        cfg = self.config
        rng = np.random.default_rng(np.random.SeedSequence([cfg.seed, _SALT_PROFILE, idx]))
        irng = np.random.default_rng(np.random.SeedSequence([cfg.seed, _SALT_INSTANCE, idx]))
        n = cfg.instances_per_service

        base_rps = float(np.clip(rng.lognormal(mean=math.log(150.0), sigma=0.8), 10.0, 1500.0))  # heavy-tailed
        cpu_base = float(rng.uniform(4.0, 12.0))
        return ServiceProfile(
            index=idx,
            name=SERVICE_NAMES[idx],
            region=cfg.regions[idx % len(cfg.regions)],
            base_rps=base_rps,
            cpu_base=cpu_base,
            target_cpu=float(rng.uniform(30.0, 50.0)),
            threads_base=float(rng.uniform(40.0, 120.0)),
            threads_extra=float(rng.uniform(20.0, 80.0)),
            mem_limit_bytes=int(rng.choice([1, 2, 4])) * 1024**3,
            mem_base_ratio=float(rng.uniform(0.35, 0.65)),
            mem_saw_amp=float(rng.uniform(0.02, 0.06)),
            mem_saw_period=float(rng.uniform(60.0, 240.0)),
            lat_base_ms=float(rng.uniform(40.0, 200.0)),
            err_base_pct=float(rng.uniform(0.05, 0.6)),
            gc_base_ms=float(rng.uniform(30.0, 150.0)),
            rx_per_req=float(rng.uniform(1_000.0, 8_000.0)),
            tx_per_req=float(rng.uniform(2_000.0, 30_000.0)),
            disk_read_base=float(rng.lognormal(math.log(2e4), 0.6)),
            disk_write_base=float(rng.lognormal(math.log(1.2e5), 0.6)),
            peak_hour=float(rng.uniform(10.0, 20.0)),
            diurnal_amp=float(rng.uniform(0.25, 0.50)),
            weekend_factor=float(rng.uniform(0.55, 0.90)),
            instance_factor=irng.lognormal(0.0, 0.08, size=n),
            instance_offset_ms=irng.integers(0, cfg.interval_seconds * 1000, size=n, dtype=np.int64),
            instance_phase=irng.uniform(0.0, 240.0, size=n),
        )

    # ---------------------------------------------------------------- generation
    @property
    def steps_per_day(self) -> int:
        return self.config.steps_per_day

    def service_day(self, service_idx: int, day_number: int) -> ServiceDay:
        """Generate (or fetch from a tiny cache) one service-day. Pure function of seed, service, day."""
        key = (service_idx, day_number)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        # keep only this service's most recent days in memory (live mode walks forward in time)
        for old in [k for k in self._cache if k[0] == service_idx and k[1] < day_number - 1]:
            del self._cache[old]
        sd = self._generate(service_idx, day_number)
        self._cache[key] = sd
        return sd

    def _generate(self, service_idx: int, day_number: int) -> ServiceDay:
        cfg, p = self.config, self.profiles[service_idx]
        n, steps = cfg.instances_per_service, cfg.steps_per_day
        rng = np.random.default_rng(np.random.SeedSequence([cfg.seed, _SALT_DAY, service_idx, day_number]))

        day_start_s = day_number * SECONDS_PER_DAY
        step_idx = np.arange(steps, dtype=np.int64)
        ts_ms = (day_start_s + step_idx * cfg.interval_seconds)[None, :] * 1000 + p.instance_offset_ms[:, None]
        hour = (ts_ms % (SECONDS_PER_DAY * 1000)) / 3_600_000.0           # UTC hour of day, (n, steps)
        weekday = (day_number + 3) % 7                                       # 1970-01-01 was a Thursday; Monday = 0
        weekend = p.weekend_factor if weekday >= 5 else 1.0

        # --- load: daily cycle x weekend x instance share x smooth noise x fast noise (mean is about 1)
        diurnal = 1.0 + p.diurnal_amp * np.cos(2.0 * math.pi * (hour - p.peak_hour) / 24.0)
        load = (
            diurnal * weekend * p.instance_factor[:, None]
            * np.exp(0.06 * _smooth(rng, n, steps, window=8))
            * np.exp(0.03 * rng.standard_normal((n, steps)))
        )
        rps = p.base_rps * load

        # --- CPU grows with traffic; calibrated so that mean traffic gives about target_cpu percent
        cpu_per_rps = (p.target_cpu - p.cpu_base) / p.base_rps
        cpu = np.clip(p.cpu_base + cpu_per_rps * rps + 1.5 * rng.standard_normal((n, steps)), 0.0, 100.0)

        threads = np.maximum(1.0, np.round(p.threads_base + p.threads_extra * load + 2.0 * rng.standard_normal((n, steps))))

        # --- memory: saw-tooth (allocation then GC) plus a slight load effect; always below 95% of the limit
        saw = ((step_idx[None, :] + p.instance_phase[:, None]) % p.mem_saw_period) / p.mem_saw_period
        mem_ratio = np.clip(
            p.mem_base_ratio + p.mem_saw_amp * saw + 0.04 * (load - 1.0) + 0.004 * rng.standard_normal((n, steps)),
            0.05, 0.95,
        )

        latency = p.lat_base_ms * (1.0 + 1.2 * (cpu / 100.0) ** 3) * np.exp(0.08 * rng.standard_normal((n, steps)))
        error = np.clip(p.err_base_pct * np.exp(0.4 * rng.standard_normal((n, steps))), 0.0, 100.0)
        gc = p.gc_base_ms * (0.7 + 0.3 * load) * np.exp(0.15 * rng.standard_normal((n, steps)))
        net_rx = rps * p.rx_per_req * np.exp(0.10 * rng.standard_normal((n, steps)))
        net_tx = rps * p.tx_per_req * np.exp(0.10 * rng.standard_normal((n, steps)))
        disk_r = p.disk_read_base * (0.8 + 0.2 * load) * np.exp(0.40 * rng.standard_normal((n, steps)))
        disk_w = p.disk_write_base * (0.7 + 0.3 * load) * np.exp(0.30 * rng.standard_normal((n, steps)))

        metrics = {
            "cpuUsagePercent": np.round(cpu, 2),
            "memoryUsedBytes": (mem_ratio * p.mem_limit_bytes).astype(np.int64),
            "memoryLimitBytes": np.full((n, steps), p.mem_limit_bytes, dtype=np.int64),
            "networkRxBytesPerSec": np.round(net_rx, 1),
            "networkTxBytesPerSec": np.round(net_tx, 1),
            "diskReadBytesPerSec": np.round(disk_r, 1),
            "diskWriteBytesPerSec": np.round(disk_w, 1),
            "activeThreadCount": threads.astype(np.int64),
            "httpRequestsPerSec": np.round(rps, 3),
            "httpErrorRatePercent": np.round(error, 3),
            "httpLatencyP95Ms": np.round(latency, 1),
            "gcPauseMsPerMin": np.round(gc, 1),
        }

        if cfg.drop_rate > 0.0:
            drop_rng = np.random.default_rng(
                np.random.SeedSequence([cfg.seed, _SALT_DROP, service_idx, day_number])
            )
            keep = drop_rng.random((n, steps)) >= cfg.drop_rate
        else:
            keep = np.ones((n, steps), dtype=bool)

        return ServiceDay(p, cfg, day_number, ts_ms, metrics, keep)


# ====================================================================== helpers
def _smooth(rng: np.random.Generator, n: int, steps: int, window: int) -> np.ndarray:
    """Unit-variance noise that changes slowly (moving sum of white noise), shape (n, steps)."""
    z = rng.standard_normal((n, steps + window - 1))
    cs = np.concatenate([np.zeros((n, 1)), np.cumsum(z, axis=1)], axis=1)
    return (cs[:, window:] - cs[:, :-window]) / math.sqrt(window)


def _uuid7(ts_ms: int, rand_a: int, rand_b: int) -> str:
    """UUID version 7 layout: 48-bit unix ms | ver 7 + 12 random bits | variant 10 + 62 random bits."""
    return "%08x-%04x-%04x-%04x-%012x" % (
        (ts_ms >> 16) & 0xFFFFFFFF,
        ts_ms & 0xFFFF,
        0x7000 | (rand_a & 0x0FFF),
        0x8000 | ((rand_b >> 48) & 0x3FFF),
        rand_b & 0xFFFFFFFFFFFF,
    )


def _uuid7_list(ts_ms: np.ndarray, rand_a: np.ndarray, rand_b: np.ndarray) -> list[str]:
    return [_uuid7(t, a, b) for t, a, b in zip(ts_ms.tolist(), rand_a.tolist(), rand_b.tolist())]


def iso_ms(ts_ms: int) -> str:
    """Epoch milliseconds -> '2026-10-12T09:15:00.000Z' (exact, no float rounding)."""
    dt = datetime.fromtimestamp(ts_ms // 1000, tz=timezone.utc)
    return f"{dt:%Y-%m-%dT%H:%M:%S}.{ts_ms % 1000:03d}Z"
