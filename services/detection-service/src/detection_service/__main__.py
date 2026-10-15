"""Entry point: `python -m detection_service`. Ctrl+C or SIGTERM stops it after the current batch."""

from __future__ import annotations

import logging
import signal
import sys
import threading

from .config import Settings
from .service import DeliveryFailed, DetectionStub


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        DetectionStub(Settings.from_env()).run(stop)
    except DeliveryFailed as err:
        logging.getLogger("detection_service").error("stopping without commit: %s", err)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
