"""Settings, read from environment variables so the same code runs on Windows and in Docker."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _default_contracts_dir() -> Path:
    """libs/telemetry-contracts of this repository (the Docker image sets CONTRACTS_DIR instead)."""
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "libs" / "telemetry-contracts"
        if candidate.is_dir():
            return candidate
    return Path("/app/contracts")


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = "localhost:9092"   # kafka:19092 inside compose
    group_id: str = "detection"                 # FR-DET-01
    input_topic: str = "telemetry.raw.v1"
    output_topic: str = "telemetry.scored.v1"
    dlq_topic: str = "telemetry.raw.v1.dlq"     # FR-DET-10 naming (open question in CLAUDE.md)
    max_batch: int = 500                        # records handled per poll, then one flush + one commit
    poll_timeout_s: float = 0.5
    flush_timeout_s: float = 30.0
    stats_interval_s: float = 10.0
    contracts_dir: Path = _default_contracts_dir()
    heartbeat_file: Path | None = None          # touched on every loop pass; the Docker HEALTHCHECK reads its age

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Settings":
        env = os.environ if env is None else env
        d = cls()
        return cls(
            bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", d.bootstrap_servers),
            group_id=env.get("DETECTION_GROUP_ID", d.group_id),
            input_topic=env.get("DETECTION_INPUT_TOPIC", d.input_topic),
            output_topic=env.get("DETECTION_OUTPUT_TOPIC", d.output_topic),
            dlq_topic=env.get("DETECTION_DLQ_TOPIC", d.dlq_topic),
            max_batch=int(env.get("DETECTION_MAX_BATCH", d.max_batch)),
            poll_timeout_s=float(env.get("DETECTION_POLL_TIMEOUT_S", d.poll_timeout_s)),
            flush_timeout_s=float(env.get("DETECTION_FLUSH_TIMEOUT_S", d.flush_timeout_s)),
            stats_interval_s=float(env.get("DETECTION_STATS_INTERVAL_S", d.stats_interval_s)),
            contracts_dir=Path(env.get("CONTRACTS_DIR", str(d.contracts_dir))),
            heartbeat_file=Path(env["DETECTION_HEARTBEAT_FILE"]) if env.get("DETECTION_HEARTBEAT_FILE") else None,
        )
