"""Command line: python -m fleet_sim <describe | generate | live> ..."""

from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from datetime import date
from pathlib import Path

from . import __version__
from .config import ConfigError, FleetConfig
from .live import LiveRunner
from .model import FleetModel
from .offline import OutputError, generate_dataset

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "default.yaml"


def _add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--config", default=str(DEFAULT_CONFIG), help="YAML config (default: configs/default.yaml)"
    )
    p.add_argument("--seed", type=int, help="override fleet.seed")
    p.add_argument(
        "--services", type=int, help="override fleet.services (use a small number for quick tests)"
    )
    p.add_argument("--instances", type=int, help="override fleet.instances_per_service")
    p.add_argument("--drop-rate", type=float, help="override sampling.drop_rate")


def _load_config(args: argparse.Namespace) -> FleetConfig:
    cfg = FleetConfig.from_yaml(args.config)
    overrides = {}
    if args.seed is not None:
        overrides["seed"] = args.seed
    if args.services is not None:
        overrides["services"] = args.services
    if args.instances is not None:
        overrides["instances_per_service"] = args.instances
    if args.drop_rate is not None:
        overrides["drop_rate"] = args.drop_rate
    return replace(cfg, **overrides) if overrides else cfg


def _cmd_describe(args: argparse.Namespace) -> int:
    cfg = _load_config(args)
    print(f"fleet_sim {__version__}  (config sha256 {cfg.sha256()[:12]})")
    print(f"  services:              {cfg.services}")
    print(f"  instances per service: {cfg.instances_per_service}  (total {cfg.instances})")
    print(f"  sampling interval:     {cfg.interval_seconds} s -> {cfg.steps_per_day:,} samples/instance/day")
    print(f"  events per day:        {cfg.events_per_day:,}  (mean {cfg.events_per_second:.1f} events/s)")
    print(f"  seed:                  {cfg.seed}     dropped samples: {cfg.drop_rate:.1%}")
    model = FleetModel(cfg)
    print("  busiest services (mean requests/s per instance):")
    for p in sorted(model.profiles, key=lambda x: -x.base_rps)[:5]:
        print(f"    {p.name:<20} {p.base_rps:8.1f} rps   region {p.region}")
    return 0


def _cmd_generate(args: argparse.Namespace) -> int:
    cfg = _load_config(args)
    start = date.fromisoformat(args.start)
    manifest = generate_dataset(
        cfg,
        args.out,
        start=start,
        days=args.days,
        fmt=args.format,
        name=args.name,
        progress=lambda m: print(m, flush=True),
    )
    t = manifest["totals"]
    print(
        f"done: {t['rows']:,} rows (expected without drops: {t['expectedRowsWithoutDrops']:,}) in {args.out}"
    )
    return 0


def _cmd_live(args: argparse.Namespace) -> int:
    cfg = _load_config(args)
    runner = LiveRunner(
        FleetModel(cfg),
        url=args.url,
        speed=args.speed,
        duration=args.duration,
        workers=args.workers,
        max_retries=args.max_retries,
        log=lambda m: print(m, flush=True),
    )
    try:
        runner.run()
    except KeyboardInterrupt:
        runner.stop()
        print("stopped: " + runner.stats.summary())
    return 0 if runner.stats.failed_batches == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fleet_sim", description="Deterministic fleet telemetry simulator")
    parser.add_argument("--version", action="version", version=f"fleet_sim {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    d = sub.add_parser("describe", help="print the fleet model and expected volumes")
    _add_common(d)
    d.set_defaults(func=_cmd_describe)

    g = sub.add_parser("generate", help="write a dataset to disk (offline mode)")
    _add_common(g)
    g.add_argument("--out", required=True, help="output directory")
    g.add_argument("--start", default="2026-01-01", help="first UTC day, YYYY-MM-DD")
    g.add_argument("--days", type=int, default=1)
    g.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    g.add_argument("--name", help="dataset name stored in the manifest (default: output directory name)")
    g.set_defaults(func=_cmd_generate)

    lv = sub.add_parser("live", help="send batches to the gateway over HTTP (live mode)")
    _add_common(lv)
    lv.add_argument("--url", default="http://localhost:8081/api/v1/telemetry/ingest")
    lv.add_argument("--speed", type=float, default=1.0, help="time compression factor (1 = real time)")
    lv.add_argument("--duration", type=float, help="seconds of REAL time to run (required if speed > 1)")
    lv.add_argument("--workers", type=int, default=16, help="parallel HTTP connections")
    lv.add_argument("--max-retries", type=int, default=5)
    lv.set_defaults(func=_cmd_live)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (ConfigError, OutputError, ValueError, FileNotFoundError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
