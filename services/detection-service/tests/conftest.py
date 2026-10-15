"""Shared test fixtures. The contract files come from libs/telemetry-contracts, the same files the gateway and simulator test."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SERVICE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_DIR / "src"))  # lets pytest run without installing the package

from detection_service.config import Settings  # noqa: E402
from detection_service.scoring import RAW_SCHEMA, SCORED_SCHEMA, load_validator  # noqa: E402

CONTRACTS = Settings().contracts_dir


@pytest.fixture(scope="session")
def raw_validator():
    return load_validator(CONTRACTS, RAW_SCHEMA)


@pytest.fixture(scope="session")
def scored_validator():
    return load_validator(CONTRACTS, SCORED_SCHEMA)


@pytest.fixture
def raw_example() -> dict:
    return json.loads((CONTRACTS / "examples" / "raw-event.json").read_text(encoding="utf-8"))
