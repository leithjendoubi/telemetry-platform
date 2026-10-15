"""Unit tests of the pure scoring logic: no Kafka needed."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from detection_service.config import Settings
from detection_service.scoring import PoisonRecord, iso_ms, parse_raw, score

NOW = datetime(2026, 10, 14, 12, 0, 0, 123456, tzinfo=timezone.utc)


def test_contracts_are_found():
    assert (Settings().contracts_dir / "schemas" / "telemetry-scored-event.1.0.json").is_file()


def test_scored_record_matches_the_contract(raw_example, scored_validator):
    scored = score(raw_example, NOW)
    assert [e.message for e in scored_validator.iter_errors(scored)] == []


def test_raw_fields_pass_through_unchanged(raw_example):
    original = json.loads(json.dumps(raw_example))
    scored = score(raw_example, NOW)
    assert {k: v for k, v in scored.items() if k not in ("prediction", "scoredAt")} == original
    assert raw_example == original, "the input dict must not be modified"


def test_stub_prediction_values(raw_example):
    scored = score(raw_example, NOW)
    assert scored["prediction"] == {
        "probability": 0.0, "isAnomaly": False, "threshold": 0.5,
        "modelVersion": "stub-0", "featureSetVersion": "none", "warmup": False,
    }
    assert scored["scoredAt"] == "2026-10-14T12:00:00.123Z"


def test_iso_ms_converts_to_utc():
    plus2 = timezone(timedelta(hours=2))
    assert iso_ms(datetime(2026, 10, 14, 14, 0, 0, 5000, tzinfo=plus2)) == "2026-10-14T12:00:00.005Z"


def test_parse_raw_accepts_a_valid_record(raw_example, raw_validator):
    assert parse_raw(json.dumps(raw_example).encode(), raw_validator) == raw_example


@pytest.mark.parametrize("value, reason", [
    (None, "empty"),
    (b"\xff\xfe", "not UTF-8 JSON"),
    (b"{not json", "not UTF-8 JSON"),
    (b"[1, 2]", "not a telemetry-raw-event"),
])
def test_unreadable_values_are_poison(value, reason, raw_validator):
    with pytest.raises(PoisonRecord, match=reason):
        parse_raw(value, raw_validator)


@pytest.mark.parametrize("change", ["drop-receivedAt", "drop-instanceId", "extra-field", "cpu-200", "bad-uuid"])
def test_records_breaking_the_raw_contract_are_poison(change, raw_example, raw_validator):
    if change == "drop-receivedAt":
        del raw_example["receivedAt"]
    elif change == "drop-instanceId":
        del raw_example["resource"]["instanceId"]
    elif change == "extra-field":
        raw_example["surprise"] = 1
    elif change == "cpu-200":
        raw_example["metrics"]["cpuUsagePercent"] = 200
    elif change == "bad-uuid":
        raw_example["eventId"] = "123"
    with pytest.raises(PoisonRecord, match="not a telemetry-raw-event 1.0 record"):
        parse_raw(json.dumps(raw_example).encode(), raw_validator)


def test_settings_from_environment():
    s = Settings.from_env({"KAFKA_BOOTSTRAP_SERVERS": "kafka:19092", "DETECTION_MAX_BATCH": "64",
                           "DETECTION_OUTPUT_TOPIC": "x.v1"})
    assert (s.bootstrap_servers, s.max_batch, s.output_topic) == ("kafka:19092", 64, "x.v1")
    assert (s.input_topic, s.group_id, s.dlq_topic) == ("telemetry.raw.v1", "detection", "telemetry.raw.v1.dlq")
    assert s.heartbeat_file is None
    assert str(Settings.from_env({"DETECTION_HEARTBEAT_FILE": "/tmp/hb"}).heartbeat_file).replace("\\", "/") == "/tmp/hb"
