package com.telemetry.analytics.query;

import com.telemetry.analytics.query.SeriesRepository.Point;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.ErrorResponseException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * GET /api/v1/analytics/instances/{instanceId}/series (spec 8.2, FR-ANA-03; contract in
 * libs/telemetry-contracts/openapi/analytics-api.yaml). Skeleton version: raw points, at most 2,000.
 */
@RestController
public class SeriesController {

    private static final Logger log = LoggerFactory.getLogger(SeriesController.class);
    private static final Pattern IDENTIFIER = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$");

    private final SeriesRepository repository;
    private final int maxPoints;
    private final Duration maxWindow;

    public SeriesController(
            SeriesRepository repository,
            @Value("${analytics.max-points}") int maxPoints,
            @Value("${analytics.max-window}") Duration maxWindow) {
        this.repository = repository;
        this.maxPoints = maxPoints;
        this.maxWindow = maxWindow;
    }

    public record Series(
            String instanceId, String metric, Instant from, Instant to, boolean truncated, List<Point> points) {}

    @GetMapping("/api/v1/analytics/instances/{instanceId}/series")
    public Series instanceSeries(
            @PathVariable String instanceId,
            @RequestParam String metric,
            @RequestParam String from,
            @RequestParam String to) {
        if (!IDENTIFIER.matcher(instanceId).matches()) {
            throw badRequest("instanceId does not match the identifier pattern");
        }
        if (!SeriesRepository.METRIC_COLUMNS.containsKey(metric)) {
            throw badRequest("unknown metric '" + metric + "'; allowed: "
                    + SeriesRepository.METRIC_COLUMNS.keySet().stream().sorted().toList());
        }
        Instant start = instant("from", from);
        Instant end = instant("to", to);
        if (!end.isAfter(start)) {
            throw badRequest("'to' must be after 'from'");
        }
        if (Duration.between(start, end).compareTo(maxWindow) > 0) {
            throw badRequest("the window may be at most " + maxWindow);
        }
        // one extra row tells whether there is more than the limit
        List<Point> points = repository.instanceSeries(instanceId, metric, start, end, maxPoints + 1);
        boolean truncated = points.size() > maxPoints;
        return new Series(instanceId, metric, start, end, truncated, truncated ? points.subList(0, maxPoints) : points);
    }

    /** Spec 6.8: when TimescaleDB is down, queries fail with 503. */
    @ExceptionHandler(DataAccessException.class)
    ProblemDetail databaseUnavailable(DataAccessException e) {
        log.warn("query failed: {}", e.getMostSpecificCause().toString());
        return ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE, "The database is unavailable, retry later");
    }

    private static Instant instant(String name, String value) {
        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeParseException e) {
            throw badRequest("'" + name + "' must be an RFC 3339 date-time, e.g. 2026-10-14T12:00:00Z");
        }
    }

    private static ErrorResponseException badRequest(String detail) {
        return new ErrorResponseException(
                HttpStatus.BAD_REQUEST, ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail), null);
    }
}
