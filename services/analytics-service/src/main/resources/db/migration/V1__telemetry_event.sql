-- telemetry_event hypertable, exactly as spec 8.4 (CAP-PFE-2026-TEL-084).
-- Compression, retention and continuous aggregates come in week 6 (FR-ANA-02) as later migrations.
-- The extension itself is created by infra/compose/init-db (it needs a superuser); IF NOT EXISTS
-- makes this line a no-op for the analytics role and documents the dependency.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE telemetry_event (
    event_time       timestamptz NOT NULL,
    event_id         uuid        NOT NULL,
    instance_id      text        NOT NULL,
    service_name     text        NOT NULL,
    environment      text        NOT NULL,
    region           text        NOT NULL,
    received_at      timestamptz NOT NULL,
    scored_at        timestamptz NOT NULL,
    cpu_usage_pct    real        NOT NULL,
    mem_used_bytes   bigint      NOT NULL,
    mem_limit_bytes  bigint      NOT NULL,
    net_rx_bps       real        NOT NULL,
    net_tx_bps       real        NOT NULL,
    disk_read_bps    real        NOT NULL,
    disk_write_bps   real        NOT NULL,
    active_threads   integer     NOT NULL,
    http_rps         real        NOT NULL,
    http_error_pct   real        NOT NULL,
    http_p95_ms      real        NOT NULL,
    gc_pause_ms_min  real        NOT NULL,
    anomaly_prob     real        NOT NULL,
    is_anomaly       boolean     NOT NULL,
    is_warmup        boolean     NOT NULL DEFAULT false,
    model_version    text        NOT NULL,
    PRIMARY KEY (instance_id, event_time, event_id)
);

SELECT create_hypertable('telemetry_event', 'event_time', chunk_time_interval => INTERVAL '1 day');

CREATE INDEX ix_event_service_time ON telemetry_event (service_name, event_time DESC);
CREATE INDEX ix_event_anomaly_time ON telemetry_event (event_time DESC) WHERE is_anomaly;
