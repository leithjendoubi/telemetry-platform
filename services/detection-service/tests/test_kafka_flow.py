"""Integration tests against the compose Kafka (localhost:9092).

Each test creates its own throw-away topics (auto-creation is off on the broker) and deletes them
afterwards, so the real telemetry.* topics are never touched. Skipped when Kafka is not reachable:
start it with the infrastructure commands of CLAUDE.md.
"""

from __future__ import annotations

import contextlib
import json
import threading
import time
import uuid
from dataclasses import replace

import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from detection_service.config import Settings
from detection_service.service import DeliveryFailed, DetectionStub

BOOTSTRAP = "localhost:9092"
pytestmark = pytest.mark.integration


def _kafka_reachable() -> bool:
    try:
        AdminClient({"bootstrap.servers": BOOTSTRAP, "socket.timeout.ms": 3000}).list_topics(timeout=5)
        return True
    except KafkaException:
        return False


if not _kafka_reachable():
    pytest.skip(f"Kafka not reachable on {BOOTSTRAP}; start infra/compose first", allow_module_level=True)


@pytest.fixture
def topics():
    """Fresh input/output/dlq topics with a unique suffix; deleted after the test."""
    suffix = uuid.uuid4().hex[:8]
    names = {"input": f"test.raw.{suffix}", "output": f"test.scored.{suffix}", "dlq": f"test.dlq.{suffix}"}
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    for f in admin.create_topics(
        [NewTopic(n, num_partitions=3, replication_factor=1) for n in names.values()]
    ).values():
        f.result()
    yield names
    for f in admin.delete_topics(list(names.values())).values():
        with contextlib.suppress(KafkaException):  # cleanup only; a leftover test topic does no harm
            f.result()


def settings_for(topics: dict, **overrides) -> Settings:
    base = Settings(
        bootstrap_servers=BOOTSTRAP,
        group_id=f"test-detection-{uuid.uuid4().hex[:8]}",
        input_topic=topics["input"],
        output_topic=topics["output"],
        dlq_topic=topics["dlq"],
        poll_timeout_s=0.2,
        stats_interval_s=60,
    )
    return replace(base, **overrides)


def produce_raw(topic: str, raw_example: dict, n: int, poison: bool = True) -> list[str]:
    """n valid raw records (unique eventIds, 5 instances), optionally one poison record. Returns eventIds."""
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    ids = []
    for i in range(n):
        record = json.loads(json.dumps(raw_example))
        record["eventId"] = str(uuid.uuid4())
        record["resource"]["instanceId"] = f"checkout-api-{i % 5:02d}"
        ids.append(record["eventId"])
        producer.produce(
            topic,
            key=record["resource"]["instanceId"],
            value=json.dumps(record),
            headers=[("traceparent", b"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")],
        )
    if poison:
        producer.produce(topic, key=b"checkout-api-00", value=b"{definitely not json")
    assert producer.flush(10) == 0
    return ids


def read_all(topic: str, expected: int, timeout_s: float = 20) -> list:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"reader-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    out, deadline = [], time.monotonic() + timeout_s
    while len(out) < expected and time.monotonic() < deadline:
        for m in consumer.consume(num_messages=500, timeout=0.5):
            if not m.error():
                out.append(m)
    # one more short poll: catches records beyond the expected count
    out += [m for m in consumer.consume(num_messages=500, timeout=1.0) if not m.error()]
    consumer.close()
    return out


# Answers of a broker whose group coordinator is still starting (right after "docker compose up").
# They mean "ask again in a moment", not "the stub did something wrong".
_COORDINATOR_NOT_READY = {
    KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
    KafkaError.COORDINATOR_NOT_AVAILABLE,
    KafkaError.NOT_COORDINATOR,
}


def committed_total(settings: Settings) -> int:
    """Sum of the group's committed offsets, or -1 while the coordinator is not ready yet."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": settings.group_id})
    try:
        parts = c.committed([TopicPartition(settings.input_topic, p) for p in range(3)], timeout=10)
    except KafkaException as err:
        if err.args[0].code() in _COORDINATOR_NOT_READY:
            return -1  # wait_for polls again; it still fails the test if commits never arrive
        raise
    finally:
        c.close()
    return sum(max(tp.offset, 0) for tp in parts)


def run_in_thread(stub: DetectionStub):
    stop, box = threading.Event(), {}

    def target():
        try:
            box["stats"] = stub.run(stop)
        except Exception as err:  # noqa: BLE001 - any error must reach the test thread, not vanish
            box["error"] = err

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return stop, thread, box


def wait_for(condition, timeout_s: float = 20) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.2)
    return False


def test_raw_records_are_scored_poison_goes_to_dlq_and_offsets_are_committed(
    topics, raw_example, scored_validator, tmp_path
):
    n = 120
    sent_ids = produce_raw(topics["input"], raw_example, n)
    heartbeat = tmp_path / "heartbeat"
    settings = settings_for(topics, heartbeat_file=heartbeat)
    stub = DetectionStub(settings)
    stop, thread, box = run_in_thread(stub)
    try:
        assert wait_for(lambda: committed_total(settings) == n + 1), (
            "offsets of all records should be committed"
        )
    finally:
        stop.set()
        thread.join(15)
    assert "error" not in box, box.get("error")
    assert box["stats"].scored == n and box["stats"].dead_lettered == 1
    assert heartbeat.is_file() and time.time() - heartbeat.stat().st_mtime < 30, (
        "loop touches the heartbeat file"
    )

    scored = read_all(topics["output"], n)
    assert len(scored) == n, "exactly one scored record per valid raw record"
    values = [json.loads(m.value()) for m in scored]
    assert sorted(v["eventId"] for v in values) == sorted(sent_ids)
    for m, v in zip(scored, values, strict=True):
        assert list(scored_validator.iter_errors(v)) == []
        assert m.key().decode() == v["resource"]["instanceId"]
        headers = dict(m.headers())
        assert headers["event-type"] == b"TelemetryScored"
        assert headers["schema-version"] == b"1.0"
        assert headers["producer"] == b"detection-service/0.1.0"
        assert headers["traceparent"].startswith(b"00-0af7651916cd43dd")
        assert v["prediction"]["modelVersion"] == "stub-0"

    dlq = read_all(topics["dlq"], 1)
    assert len(dlq) == 1
    assert dlq[0].value() == b"{definitely not json"
    assert dlq[0].key() == b"checkout-api-00"
    h = dict(dlq[0].headers())
    assert b"not UTF-8 JSON" in h["dlq-error"] and h["dlq-attempts"] == b"1"
    assert h["dlq-source-topic"].decode() == topics["input"]

    # a restart of the same group finds nothing new to do: committed work is not redone
    again = DetectionStub(settings)
    stop2, thread2, box2 = run_in_thread(again)
    time.sleep(4)
    stop2.set()
    thread2.join(15)
    assert box2["stats"].consumed == 0


def test_output_refused_means_no_commit(topics, raw_example):
    """If Kafka refuses the outputs, the stub stops WITHOUT committing, so the inputs are read again later."""
    produce_raw(topics["input"], raw_example, 10, poison=False)
    settings = settings_for(topics, output_topic=f"does.not.exist.{uuid.uuid4().hex[:8]}", flush_timeout_s=3)
    stub = DetectionStub(settings)
    stop, thread, box = run_in_thread(stub)
    thread.join(60)
    stop.set()
    assert isinstance(box.get("error"), DeliveryFailed)
    assert wait_for(lambda: committed_total(settings) >= 0), "group coordinator never became ready"
    assert committed_total(settings) == 0
