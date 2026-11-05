package com.telemetry.analytics.ingest;

import java.time.OffsetDateTime;
import java.util.UUID;

/** One row of telemetry_event (spec 8.4), in column order. */
public record TelemetryRow(
        OffsetDateTime eventTime,
        UUID eventId,
        String instanceId,
        String serviceName,
        String environment,
        String region,
        OffsetDateTime receivedAt,
        OffsetDateTime scoredAt,
        double cpuUsagePct,
        long memUsedBytes,
        long memLimitBytes,
        double netRxBps,
        double netTxBps,
        double diskReadBps,
        double diskWriteBps,
        int activeThreads,
        double httpRps,
        double httpErrorPct,
        double httpP95Ms,
        double gcPauseMsMin,
        double anomalyProb,
        boolean isAnomaly,
        boolean isWarmup,
        String modelVersion) {}
