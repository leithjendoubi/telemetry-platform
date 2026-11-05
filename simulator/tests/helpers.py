"""Shared test helpers: paths, the contract schema, and the semantic rules JSON Schema cannot express."""

from __future__ import annotations

import copy
import json
import math
import re
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

SIM_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SIM_DIR.parent
sys.path.insert(0, str(SIM_DIR / "src"))  # lets `python -m unittest` work without installing the package

CONTRACTS = REPO_ROOT / "libs" / "telemetry-contracts"
SCHEMA_PATH = CONTRACTS / "schemas" / "telemetry-batch.1.0.json"
VALID_EXAMPLE = CONTRACTS / "examples" / "valid-batch.json"
INVALID_CASES = CONTRACTS / "examples" / "invalid-cases.json"
RAW_EVENT_SCHEMA_PATH = CONTRACTS / "schemas" / "telemetry-raw-event.1.0.json"
RAW_EVENT_EXAMPLE = CONTRACTS / "examples" / "raw-event.json"
SCORED_EVENT_SCHEMA_PATH = CONTRACTS / "schemas" / "telemetry-scored-event.1.0.json"
SCORED_EVENT_EXAMPLE = CONTRACTS / "examples" / "scored-event.json"
REJECTED_EVENT_SCHEMA_PATH = CONTRACTS / "schemas" / "telemetry-rejected-event.1.0.json"
REJECTED_EVENT_EXAMPLE = CONTRACTS / "examples" / "rejected-event.json"
CONTRACTS_README = CONTRACTS / "README.md"
ANALYTICS_OPENAPI_PATH = CONTRACTS / "openapi" / "analytics-api.yaml"
DEFAULT_CONFIG = SIM_DIR / "configs" / "default.yaml"

ISO_MS = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$")
UUID_V7 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def ecma_anchors(pattern: str) -> str:
    r"""Rewrites a JSON Schema pattern for Python's re: an unescaped '$' outside [...] becomes '\Z'.

    JSON Schema patterns are ECMA-262, where '$' is the very end of the text; in Python (and Java) '$'
    also matches before a final newline, so "id\n" would pass '^[a-z]+$' (contract README, rule 4).
    The gateway does the same.
    """
    out, i, in_class = [], 0, False
    while i < len(pattern):
        c = pattern[i]
        if c == "\\":
            out.append(pattern[i : i + 2])
            i += 2
            continue
        if in_class:
            in_class = c != "]"
        elif c == "[":
            in_class = True
        elif c == "$":
            c = r"\Z"
        out.append(c)
        i += 1
    return "".join(out)


def make_validator():
    """JSON Schema draft 2020-12 validator with format checking (uuid, ...) and ECMA-262 '$' in patterns."""
    from jsonschema import Draft202012Validator, FormatChecker, ValidationError, validators

    def pattern(validator, regex, instance, schema):
        if validator.is_type(instance, "string") and not re.search(ecma_anchors(regex), instance):
            yield ValidationError(f"{instance!r} does not match {regex!r}")

    ecma_validator = validators.extend(Draft202012Validator, {"pattern": pattern})
    return ecma_validator(load_json(SCHEMA_PATH), format_checker=FormatChecker())


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def iso_ms_dt(dt: datetime) -> str:
    dt = dt.astimezone(UTC)
    return f"{dt:%Y-%m-%dT%H:%M:%S}.{dt.microsecond // 1000:03d}Z"


def semantic_errors(event: dict, now: datetime) -> list[str]:
    """Per-event rules of spec 8.1 that JSON Schema cannot check (the gateway implements the same rules)."""
    errors = []
    ts = parse_ts(event["timestamp"])
    if ts < now - timedelta(hours=24) or ts > now + timedelta(minutes=5):
        errors.append("TIMESTAMP_OUT_OF_RANGE")
    metrics = event["metrics"]
    if metrics["memoryUsedBytes"] > metrics["memoryLimitBytes"]:
        errors.append("SEMANTIC_VIOLATION")
    if any(isinstance(v, float) and not math.isfinite(v) for v in metrics.values()):
        errors.append("SEMANTIC_VIOLATION")  # e.g. 1e400, which json turns into inf (FR-ING-04)
    return errors


# ---------------------------------------------------------------- invalid-case patching
def _walk(doc, parts):
    for part in parts[:-1]:
        doc = doc[int(part)] if isinstance(doc, list) else doc[part]
    return doc, parts[-1]


def _resolve_relative(value, now: datetime):
    """'RELATIVE:-25h' / 'RELATIVE:+10m' -> ISO timestamp relative to `now`; 'NUMBER:1e400' -> that number."""
    if isinstance(value, str) and value.startswith("NUMBER:"):
        return json.loads(value.removeprefix("NUMBER:"))
    if isinstance(value, str) and value.startswith("RELATIVE:"):
        m = re.fullmatch(r"RELATIVE:([+-])(\d+)([hm])", value)
        amount = int(m.group(2)) * (3600 if m.group(3) == "h" else 60)
        delta = timedelta(seconds=amount if m.group(1) == "+" else -amount)
        return iso_ms_dt(now + delta)
    return value


def apply_case(base: dict, case: dict, now: datetime) -> dict:
    doc = copy.deepcopy(base)
    for pointer, value in case.get("set", {}).items():
        parts = pointer.strip("/").split("/")
        parent, key = _walk(doc, parts)
        if isinstance(parent, list):
            parent[int(key)] = _resolve_relative(value, now)
        else:
            parent[key] = _resolve_relative(value, now)
    for pointer in case.get("remove", []):
        parts = pointer.strip("/").split("/")
        parent, key = _walk(doc, parts)
        del parent[int(key) if isinstance(parent, list) else key]
    return doc
