"""The Kafka loop: consume telemetry.raw.v1, score, publish telemetry.scored.v1, then commit.

Delivery guarantee: at-least-once (spec ADR-007).
    poll up to max_batch records -> produce one output per record -> flush (wait for broker acks)
    -> commit the consumer offsets.
Offsets are committed only after every output was acknowledged. If the process dies in between, the
records are read again after restart and scored twice; the analytics writer's ON CONFLICT DO NOTHING
makes that harmless. If Kafka refuses an output, the loop stops WITHOUT committing (DeliveryFailed), so
nothing is skipped; the container restarts and resumes from the last commit (spec 6.8).

Poison records (not JSON, or not matching telemetry-raw-event 1.0) are copied to the DLQ with the reason
(FR-DET-10) and the loop moves on. Retrying them could never help, so the attempt count is 1.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer

from . import __version__
from .config import Settings
from .scoring import RAW_SCHEMA, PoisonRecord, iso_ms, load_validator, parse_raw, score

log = logging.getLogger("detection_service")

PRODUCER_NAME = f"detection-service/{__version__}"
EVENT_TYPE = "TelemetryScored"
SCHEMA_VERSION = "1.0"


class DeliveryFailed(RuntimeError):
    """Kafka did not acknowledge an output record. Offsets were not committed."""


@dataclass
class Stats:
    consumed: int = 0
    scored: int = 0
    dead_lettered: int = 0
    batches: int = 0

    def line(self) -> str:
        return f"consumed={self.consumed} scored={self.scored} dead_lettered={self.dead_lettered} batches={self.batches}"


class DetectionStub:
    def __init__(self, settings: Settings, clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        self.settings = settings
        self.clock = clock
        self.stats = Stats()
        self.validator = load_validator(settings.contracts_dir, RAW_SCHEMA)
        self.consumer = Consumer({
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": settings.group_id,
            "client.id": "detection-service",
            "enable.auto.commit": False,          # we commit ourselves, after the outputs are stored
            "auto.offset.reset": "earliest",      # a new group starts at the oldest retained record
            "partition.assignment.strategy": "cooperative-sticky",
        })
        self.producer = Producer({
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": "detection-service",
            "enable.idempotence": True,           # implies acks=all; client retries never duplicate
            "acks": "all",
            "linger.ms": 5,
            "compression.type": "lz4",
        })
        self._delivery_errors: list[str] = []

    # ------------------------------------------------------------------ main loop
    def run(self, stop: threading.Event) -> Stats:
        s = self.settings
        self.consumer.subscribe([s.input_topic])
        log.info("consuming %s (group %s) -> %s, dlq %s, kafka %s",
                 s.input_topic, s.group_id, s.output_topic, s.dlq_topic, s.bootstrap_servers)
        next_stats = time.monotonic() + s.stats_interval_s
        try:
            while not stop.is_set():
                self._heartbeat()
                messages = self.consumer.consume(num_messages=s.max_batch, timeout=s.poll_timeout_s)
                if messages:
                    self.process_batch(messages)
                if time.monotonic() >= next_stats:
                    log.info("stats %s", self.stats.line())
                    next_stats = time.monotonic() + s.stats_interval_s
        finally:
            self.close()
        log.info("stopped: %s", self.stats.line())
        return self.stats

    def process_batch(self, messages: list[Message]) -> None:
        self._delivery_errors.clear()
        handled = 0
        for msg in messages:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            self.stats.consumed += 1
            handled += 1
            self._handle(msg)
            self.producer.poll(0)  # serve delivery callbacks while producing
        if handled == 0:
            return
        remaining = self.producer.flush(self.settings.flush_timeout_s)
        if remaining or self._delivery_errors:
            raise DeliveryFailed(f"{remaining} records unacknowledged, errors: {self._delivery_errors[:3]}")
        self.consumer.commit(asynchronous=False)
        self.stats.batches += 1

    def _handle(self, msg: Message) -> None:
        try:
            raw = parse_raw(msg.value(), self.validator)
        except PoisonRecord as err:
            self._dead_letter(msg, str(err))
            return
        scored = score(raw, self.clock())
        headers = [
            ("event-type", EVENT_TYPE.encode()),
            ("schema-version", SCHEMA_VERSION.encode()),
            ("produced-at", iso_ms(self.clock()).encode()),
            ("producer", PRODUCER_NAME.encode()),
        ]
        for name, value in msg.headers() or []:
            if name == "traceparent" and value is not None:
                headers.append((name, value))
        self.producer.produce(
            self.settings.output_topic,
            key=raw["resource"]["instanceId"].encode("utf-8"),
            value=json.dumps(scored, separators=(",", ":")).encode("utf-8"),
            headers=headers,
            on_delivery=self._on_delivery,
        )
        self.stats.scored += 1

    def _dead_letter(self, msg: Message, reason: str) -> None:
        log.warning("poison record %s[%d]@%d -> %s: %s",
                    msg.topic(), msg.partition(), msg.offset(), self.settings.dlq_topic, reason)
        headers = list(msg.headers() or []) + [
            ("dlq-error", reason.encode("utf-8")),
            ("dlq-attempts", b"1"),
            ("dlq-source-topic", msg.topic().encode()),
            ("dlq-source-partition", str(msg.partition()).encode()),
            ("dlq-source-offset", str(msg.offset()).encode()),
            ("producer", PRODUCER_NAME.encode()),
        ]
        self.producer.produce(self.settings.dlq_topic, key=msg.key(), value=msg.value(),
                              headers=headers, on_delivery=self._on_delivery)
        self.stats.dead_lettered += 1

    def _heartbeat(self) -> None:
        """Proof of life for the container health check: the loop is still turning."""
        if self.settings.heartbeat_file is not None:
            self.settings.heartbeat_file.touch()

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self._delivery_errors.append(f"{msg.topic()}: {err}")

    def close(self) -> None:
        try:
            self.producer.flush(self.settings.flush_timeout_s)
        finally:
            self.consumer.close()  # leaves the group cleanly so partitions move at once
