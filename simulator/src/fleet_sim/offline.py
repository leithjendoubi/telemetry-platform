"""Offline mode (FR-SIM-01): write a labelled dataset to disk, one file per day, plus a manifest.

Output layout::

    <out>/manifest.json            what was generated: seed, config, row counts, hashes (spec 9.1, D-06)
    <out>/incidents.jsonl          ground-truth incidents (empty in v0: normal traffic only)
    <out>/date=2026-01-01/part-0000.parquet   (or part-0000.csv.gz)

Rows inside a file are sorted by event_time (ties: service order, then instance), so the files can be
replayed in time order and split chronologically for machine learning (spec 9.3).
"""

from __future__ import annotations

import hashlib
import json
import platform
import sys
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path

import numpy as np

from . import MODEL_VERSION, __version__
from .config import FleetConfig
from .model import METRICS, FleetModel

EPOCH = date(1970, 1, 1)

# Column order of every output file. Matches the telemetry_event table of spec 8.4 where it overlaps.
COLUMNS: tuple[str, ...] = (
    "event_time",
    "event_id",
    "instance_id",
    "service_name",
    "environment",
    "region",
    "interval_seconds",
    *(column for _, column in METRICS),
    "label",
)
_STRING_COLUMNS = ("event_id", "instance_id", "service_name", "environment", "region")


class OutputError(RuntimeError):
    pass


def generate_dataset(
    config: FleetConfig,
    out_dir: str | Path,
    start: date,
    days: int,
    fmt: str = "parquet",
    name: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict:
    """Generate `days` days starting at `start` (UTC). Returns the manifest (also written to disk)."""
    if days < 1:
        raise OutputError("days must be at least 1")
    if fmt not in ("parquet", "csv"):
        raise OutputError("format must be 'parquet' or 'csv'")
    writer = _parquet_writer if fmt == "parquet" else _csv_writer
    if fmt == "parquet":
        _require_pyarrow()

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    say = progress or (lambda _msg: None)

    model = FleetModel(config)
    files: list[dict] = []
    day_digests: list[str] = []
    total_rows = 0

    for offset in range(days):
        day = start + timedelta(days=offset)
        day_number = (day - EPOCH).days
        say(f"{day.isoformat()}: generating {config.services} services ...")

        parts = [model.service_day(s, day_number).rows() for s in range(config.services)]
        cols = {key: np.concatenate([p[key] for p in parts]) for key in parts[0]}
        order = np.argsort(cols["event_time_ms"], kind="stable")  # stable: ties keep service/instance order
        cols = {key: value[order] for key, value in cols.items()}
        rows = len(order)

        rel = Path(f"date={day.isoformat()}") / f"part-0000.{'parquet' if fmt == 'parquet' else 'csv.gz'}"
        path = out / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        writer(path, cols)

        content = content_digest(cols)
        day_digests.append(content)
        total_rows += rows
        files.append(
            {
                "path": rel.as_posix(),
                "rows": rows,
                "bytes": path.stat().st_size,
                "sha256": _file_sha256(path),
                "contentSha256": content,
            }
        )
        say(f"{day.isoformat()}: wrote {rows:,} rows -> {rel.as_posix()}")
        model._cache.clear()  # free memory between days

    positives = 0  # v0 has no faults
    manifest = {
        "generator": {"name": "fleet_sim", "version": __version__, "modelVersion": MODEL_VERSION},
        "dataset": {
            "name": name or out.name,
            "start": start.isoformat(),
            "days": days,
            "format": fmt,
            "sorted": "event_time, then service order, then instance",
        },
        "config": config.to_dict(),
        "configSha256": config.sha256(),
        "totals": {
            "rows": total_rows,
            "expectedRowsWithoutDrops": config.events_per_day * days,
            "labelPositive": positives,
            "labelPrevalence": positives / total_rows if total_rows else 0.0,
        },
        "columns": list(COLUMNS),
        "files": files,
        # Portable fingerprint of the DATA (independent of file format and writer versions).
        # Two runs with the same seed, config and numpy version must give the same value.
        "datasetContentSha256": hashlib.sha256("\n".join(day_digests).encode()).hexdigest(),
        # Informational only. Not part of the reproducibility comparison.
        "environment": _environment(),
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (out / "incidents.jsonl").write_text("", encoding="utf-8")  # FR-SIM-04; filled in stage 3
    digest = str(manifest["datasetContentSha256"])[:16]
    say(f"manifest written: {total_rows:,} rows, datasetContentSha256={digest}...")
    return manifest


# ====================================================================== digests
def content_digest(cols: dict[str, np.ndarray]) -> str:
    """SHA-256 over the column VALUES (not the file bytes), so CSV and Parquet runs can be compared."""
    h = hashlib.sha256()
    for key in ("event_time_ms", *COLUMNS[1:]):
        arr = cols[key]
        h.update(key.encode())
        if key in _STRING_COLUMNS:
            h.update("\x1f".join(arr.tolist()).encode("utf-8"))
        else:
            h.update(np.ascontiguousarray(arr).astype(arr.dtype.newbyteorder("<"), copy=False).tobytes())
    return h.hexdigest()


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _environment() -> dict:
    env = {"python": platform.python_version(), "numpy": np.__version__}
    try:
        import pandas

        env["pandas"] = pandas.__version__
    except ImportError:
        pass
    try:
        import pyarrow

        env["pyarrow"] = pyarrow.__version__
    except ImportError:
        pass
    return env


# ====================================================================== writers
def _csv_writer(path: Path, cols: dict[str, np.ndarray]) -> None:
    import pandas as pd

    frame = pd.DataFrame({"event_time": pd.to_datetime(cols["event_time_ms"], unit="ms", utc=True)})
    for key in COLUMNS[1:]:
        frame[key] = cols[key]
    # mtime=0 keeps the gzip header constant, so the same data gives the same file bytes
    frame.to_csv(
        path,
        index=False,
        compression={"method": "gzip", "mtime": 0},
        date_format="%Y-%m-%dT%H:%M:%S.%fZ",
    )


def _parquet_writer(path: Path, cols: dict[str, np.ndarray]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    arrays = {
        "event_time": pa.array(
            cols["event_time_ms"].astype("datetime64[ms]"), type=pa.timestamp("ms", tz="UTC")
        )
    }
    for key in COLUMNS[1:]:
        arrays[key] = pa.array(cols[key])
    pq.write_table(pa.table(arrays), path, compression="zstd")


def _require_pyarrow() -> None:
    try:
        import pyarrow  # noqa: F401
    except ImportError as exc:
        raise OutputError(
            "Parquet output needs pyarrow (pip install pyarrow). Install it, or run again with --format csv."
        ) from exc


def python_info() -> str:
    return f"python {sys.version.split()[0]}"
