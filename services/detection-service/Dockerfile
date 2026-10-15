# syntax=docker/dockerfile:1
# Detection service image, pass-through stub (spec 14.2): multi-stage build, slim Python 3.12, non-root.
# Build context = repository root, because the image carries libs/telemetry-contracts:
#   docker build -f services/detection-service/Dockerfile .
# TODO (spec 14.1): install from a lock file once the real service adds its ML dependencies.

FROM python:3.12.15-slim-trixie AS build
RUN python -m venv /opt/venv
COPY services/detection-service/pyproject.toml /src/pyproject.toml
COPY services/detection-service/src /src/src
RUN --mount=type=cache,target=/root/.cache/pip /opt/venv/bin/pip install /src

FROM python:3.12.15-slim-trixie
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app
COPY --from=build /opt/venv /opt/venv
COPY libs/telemetry-contracts /app/contracts
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CONTRACTS_DIR=/app/contracts \
    DETECTION_HEARTBEAT_FILE=/tmp/heartbeat
USER 10001
# healthy = the consume loop touched the heartbeat file within the last 30 s (the stub has no HTTP port)
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
  CMD ["python", "-c", "import os, sys, time; sys.exit(0 if time.time() - os.path.getmtime('/tmp/heartbeat') < 30 else 1)"]
ENTRYPOINT ["python", "-m", "detection_service"]
