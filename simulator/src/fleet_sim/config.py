"""Simulator configuration: the fleet model of spec 4.1, loaded from YAML."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

# 35 logical services (spec 4.1). Names match the identifier pattern ^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$
SERVICE_NAMES: tuple[str, ...] = (
    "checkout-api",
    "cart-api",
    "catalog-api",
    "search-api",
    "pricing-api",
    "inventory-api",
    "order-api",
    "payment-api",
    "shipping-api",
    "notification-api",
    "user-api",
    "auth-api",
    "profile-api",
    "review-api",
    "recommendation-api",
    "media-api",
    "upload-api",
    "email-worker",
    "sms-worker",
    "report-worker",
    "billing-api",
    "invoice-api",
    "tax-api",
    "fraud-api",
    "loyalty-api",
    "coupon-api",
    "wishlist-api",
    "analytics-ingest",
    "audit-api",
    "config-api",
    "gateway-edge",
    "session-api",
    "geo-api",
    "support-api",
    "returns-api",
)

_RESERVED_EMPTY = ("scenarios", "confounders")


class ConfigError(ValueError):
    """The configuration file is invalid."""


@dataclass(frozen=True)
class FleetConfig:
    seed: int = 42
    services: int = 35
    instances_per_service: int = 10
    interval_seconds: int = 15
    environment: str = "prod"
    regions: tuple[str, ...] = ("eu-south-1", "eu-west-1", "eu-central-1")
    drop_rate: float = 0.0
    agent_version: str = "1.0.0"
    api_key_template: str = "dev-{agent_id}"

    def __post_init__(self) -> None:
        if not 1 <= self.services <= len(SERVICE_NAMES):
            raise ConfigError(f"services must be between 1 and {len(SERVICE_NAMES)}, got {self.services}")
        if not 1 <= self.instances_per_service <= 99:
            raise ConfigError("instances_per_service must be between 1 and 99")
        if not 1 <= self.interval_seconds <= 300 or 86_400 % self.interval_seconds != 0:
            raise ConfigError("interval_seconds must be 1..300 and divide 86400 evenly (e.g. 15)")
        if self.environment not in ("dev", "staging", "prod"):
            raise ConfigError("environment must be dev, staging or prod")
        if not self.regions:
            raise ConfigError("regions must not be empty")
        if not 0.0 <= self.drop_rate < 1.0:
            raise ConfigError("drop_rate must be in [0, 1)")

    # ------------------------------------------------------------------ derived numbers
    @property
    def steps_per_day(self) -> int:
        return 86_400 // self.interval_seconds

    @property
    def instances(self) -> int:
        return self.services * self.instances_per_service

    @property
    def events_per_day(self) -> int:
        """Expected daily volume without dropped samples: 350 x 5,760 = 2,016,000 for the default fleet."""
        return self.instances * self.steps_per_day

    @property
    def events_per_second(self) -> float:
        return self.instances / self.interval_seconds

    def agent_id(self, service_idx: int) -> str:
        return f"agent-{SERVICE_NAMES[service_idx]}"

    def api_key(self, service_idx: int) -> str:
        return self.api_key_template.format(agent_id=self.agent_id(service_idx))

    # ------------------------------------------------------------------ serialisation
    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["regions"] = list(self.regions)
        return d

    def sha256(self) -> str:
        canonical = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FleetConfig:
        """Accepts the nested YAML layout (fleet / sampling / agent) described in configs/default.yaml."""
        data = dict(data or {})
        for key in _RESERVED_EMPTY:
            if data.pop(key, None):
                raise ConfigError(
                    f"'{key}' is not supported yet: fault scenarios and confounders arrive in stage 3. "
                    "Leave it empty for now."
                )
        fleet = dict(data.pop("fleet", {}) or {})
        sampling = dict(data.pop("sampling", {}) or {})
        agent = dict(data.pop("agent", {}) or {})
        if data:
            raise ConfigError(f"unknown top-level keys: {sorted(data)}")

        kwargs: dict[str, Any] = {}
        _take(kwargs, fleet, ["seed", "services", "instances_per_service", "interval_seconds", "environment"])
        if "regions" in fleet:
            kwargs["regions"] = tuple(fleet.pop("regions"))
        _take(kwargs, sampling, ["drop_rate"])
        if "version" in agent:
            kwargs["agent_version"] = agent.pop("version")
        _take(kwargs, agent, ["api_key_template"])
        for section, leftovers in (("fleet", fleet), ("sampling", sampling), ("agent", agent)):
            if leftovers:
                raise ConfigError(f"unknown keys in '{section}': {sorted(leftovers)}")
        return cls(**kwargs)

    @classmethod
    def from_yaml(cls, path: str | Path) -> FleetConfig:
        with open(path, encoding="utf-8") as fh:
            return cls.from_dict(yaml.safe_load(fh) or {})


def _take(dest: dict[str, Any], source: dict[str, Any], keys: list[str]) -> None:
    for key in keys:
        if key in source:
            dest[key] = source.pop(key)
