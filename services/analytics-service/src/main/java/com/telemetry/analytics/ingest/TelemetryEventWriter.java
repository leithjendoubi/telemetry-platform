package com.telemetry.analytics.ingest;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Batch-inserts rows into telemetry_event in ONE transaction (FR-ANA-01).
 * {@code ON CONFLICT DO NOTHING} makes a replayed event harmless: the primary key
 * (instance_id, event_time, event_id) already exists, so the second copy is skipped.
 */
public class TelemetryEventWriter {

    static final String INSERT = """
            INSERT INTO telemetry_event (
                event_time, event_id, instance_id, service_name, environment, region, received_at, scored_at,
                cpu_usage_pct, mem_used_bytes, mem_limit_bytes, net_rx_bps, net_tx_bps, disk_read_bps, disk_write_bps,
                active_threads, http_rps, http_error_pct, http_p95_ms, gc_pause_ms_min,
                anomaly_prob, is_anomaly, is_warmup, model_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;

    public TelemetryEventWriter(JdbcTemplate jdbc, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.tx = tx;
    }

    /** Returns how many rows were new; the rest were duplicates. Throws if the database fails (nothing is stored then). */
    public int write(List<TelemetryRow> rows) {
        if (rows.isEmpty()) {
            return 0;
        }
        int[] counts = tx.execute(status -> jdbc.batchUpdate(INSERT, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                TelemetryRow r = rows.get(i);
                int c = 1;
                ps.setObject(c++, r.eventTime());
                ps.setObject(c++, r.eventId());
                ps.setString(c++, r.instanceId());
                ps.setString(c++, r.serviceName());
                ps.setString(c++, r.environment());
                ps.setString(c++, r.region());
                ps.setObject(c++, r.receivedAt());
                ps.setObject(c++, r.scoredAt());
                ps.setFloat(c++, (float) r.cpuUsagePct());
                ps.setLong(c++, r.memUsedBytes());
                ps.setLong(c++, r.memLimitBytes());
                ps.setFloat(c++, (float) r.netRxBps());
                ps.setFloat(c++, (float) r.netTxBps());
                ps.setFloat(c++, (float) r.diskReadBps());
                ps.setFloat(c++, (float) r.diskWriteBps());
                ps.setInt(c++, r.activeThreads());
                ps.setFloat(c++, (float) r.httpRps());
                ps.setFloat(c++, (float) r.httpErrorPct());
                ps.setFloat(c++, (float) r.httpP95Ms());
                ps.setFloat(c++, (float) r.gcPauseMsMin());
                ps.setFloat(c++, (float) r.anomalyProb());
                ps.setBoolean(c++, r.isAnomaly());
                ps.setBoolean(c++, r.isWarmup());
                ps.setString(c, r.modelVersion());
            }

            @Override
            public int getBatchSize() {
                return rows.size();
            }
        }));
        return Arrays.stream(counts).filter(n -> n > 0).sum();
    }
}
