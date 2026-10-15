package com.telemetry.analytics.query;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** Reads series out of telemetry_event. */
@Repository
public class SeriesRepository {

    /**
     * Contract metric name (telemetry-batch.1.0.json) -> column of spec 8.4. Only these strings are ever put
     * into SQL, so the metric parameter cannot inject SQL.
     */
    public static final Map<String, String> METRIC_COLUMNS = Map.ofEntries(
            Map.entry("cpuUsagePercent", "cpu_usage_pct"),
            Map.entry("memoryUsedBytes", "mem_used_bytes"),
            Map.entry("memoryLimitBytes", "mem_limit_bytes"),
            Map.entry("networkRxBytesPerSec", "net_rx_bps"),
            Map.entry("networkTxBytesPerSec", "net_tx_bps"),
            Map.entry("diskReadBytesPerSec", "disk_read_bps"),
            Map.entry("diskWriteBytesPerSec", "disk_write_bps"),
            Map.entry("activeThreadCount", "active_threads"),
            Map.entry("httpRequestsPerSec", "http_rps"),
            Map.entry("httpErrorRatePercent", "http_error_pct"),
            Map.entry("httpLatencyP95Ms", "http_p95_ms"),
            Map.entry("gcPauseMsPerMin", "gc_pause_ms_min"));

    public record Point(Instant time, double value, double anomalyProb, boolean isAnomaly) {
    }

    private final JdbcTemplate jdbc;

    public SeriesRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Points of [from, to) in time order, at most {@code limit}. Uses the primary key (instance_id, event_time, ...). */
    public List<Point> instanceSeries(String instanceId, String metric, Instant from, Instant to, int limit) {
        String column = METRIC_COLUMNS.get(metric);
        if (column == null) {
            throw new IllegalArgumentException("unknown metric " + metric);
        }
        String sql = "SELECT event_time, " + column + " AS value, anomaly_prob, is_anomaly FROM telemetry_event"
                + " WHERE instance_id = ? AND event_time >= ? AND event_time < ? ORDER BY event_time LIMIT ?";
        return jdbc.query(sql, (rs, i) -> new Point(
                        rs.getTimestamp(1).toInstant(), rs.getDouble(2), rs.getDouble(3), rs.getBoolean(4)),
                instanceId, Timestamp.from(from), Timestamp.from(to), limit);
    }
}
