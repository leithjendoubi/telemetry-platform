"""Turning one raw record into one scored record. No Kafka here, so it is easy to test.

The stub gives every event the same "nothing unusual" prediction. The real model (weeks 9-13) replaces
`stub_prediction` and keeps everything else: the record shape is fixed by libs/telemetry-contracts.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

RAW_SCHEMA = "schemas/telemetry-raw-event.1.0.json"
SCORED_SCHEMA = "schemas/telemetry-scored-event.1.0.json"

STUB_MODEL_VERSION = "stub-0"
STUB_FEATURE_SET_VERSION = "none"
STUB_THRESHOLD = 0.5   # placeholder; a real threshold is chosen on the validation day (spec 9)


class PoisonRecord(Exception):
    """A record that can never be processed (not JSON, or not a telemetry.raw.v1 record). Goes to the DLQ."""


def load_validator(contracts_dir: Path, schema: str) -> Draft202012Validator:
    path = contracts_dir / schema
    with path.open(encoding="utf-8") as f:
        return Draft202012Validator(json.load(f), format_checker=FormatChecker())


def parse_raw(value: bytes | None, validator: Draft202012Validator) -> dict[str, Any]:
    """Decode and check one telemetry.raw.v1 value. Raises PoisonRecord with a short reason."""
    if value is None:
        raise PoisonRecord("empty record value")
    try:
        record = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as err:
        raise PoisonRecord(f"not UTF-8 JSON: {err}") from None
    errors = sorted(validator.iter_errors(record), key=lambda e: list(e.absolute_path))
    if errors:
        first = errors[0]
        where = "/" + "/".join(str(p) for p in first.absolute_path)
        raise PoisonRecord(f"not a telemetry-raw-event 1.0 record: {where}: {first.message}"[:500])
    return record


def stub_prediction() -> dict[str, Any]:
    return {
        "probability": 0.0,
        "isAnomaly": False,
        "threshold": STUB_THRESHOLD,
        "modelVersion": STUB_MODEL_VERSION,
        "featureSetVersion": STUB_FEATURE_SET_VERSION,
        "warmup": False,
    }


def score(raw: dict[str, Any], now: datetime) -> dict[str, Any]:
    """The raw record unchanged, plus prediction and scoredAt (telemetry-scored-event 1.0)."""
    scored = dict(raw)
    scored["prediction"] = stub_prediction()
    scored["scoredAt"] = iso_ms(now)
    return scored


def iso_ms(moment: datetime) -> str:
    """2026-10-14T13:24:14.458Z, the timestamp style used across the platform."""
    moment = moment.astimezone(timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"
