package com.telemetry.gateway.metrics;

import com.telemetry.gateway.ratelimit.InFlightLimiter;
import com.telemetry.gateway.validation.RejectionCode;
import com.telemetry.gateway.validation.ValidationOutcome.Rejection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * The gateway's own metrics of spec 12.2, at /metrics in Prometheus format (FR-ING-09). Request rate, errors and
 * duration (RED) come from Spring Boot's http_server_requests_seconds. No agent id as a label: with up to 10,000
 * agents that would multiply every series (high cardinality).
 *
 * <pre>
 *  gateway_events_accepted_total               events written to telemetry.raw.v1 (counted after Kafka's ack)
 *  gateway_events_rejected_total{code}         events rejected, by reason code (also on telemetry.rejected.v1)
 *  gateway_requests_rate_limited_total         requests answered 429 (an agent over its quota)
 *  gateway_requests_overloaded_total           requests answered 503 because 2,000 were in progress
 *  gateway_requests_in_flight                  requests in progress now
 *  gateway_kafka_publish_seconds{outcome}      time until Kafka acknowledged a batch's records
 * </pre>
 */
public class GatewayMetrics {

    private final Counter accepted;
    private final Map<RejectionCode, Counter> rejected = new EnumMap<>(RejectionCode.class);
    private final Counter rateLimited;
    private final Counter overloaded;
    private final Timer publishOk;
    private final Timer publishFailed;

    public GatewayMetrics(MeterRegistry registry, InFlightLimiter inFlight) {
        accepted = Counter.builder("gateway.events.accepted")
                .description("Events written to telemetry.raw.v1")
                .register(registry);
        for (RejectionCode code : RejectionCode.values()) { // every code exists from the start, also at 0
            rejected.put(
                    code,
                    Counter.builder("gateway.events.rejected")
                            .description("Events rejected, by reason code")
                            .tag("code", code.name())
                            .register(registry));
        }
        rateLimited = Counter.builder("gateway.requests.rate.limited")
                .description("Requests answered 429: an agent over its rate limit")
                .register(registry);
        overloaded = Counter.builder("gateway.requests.overloaded")
                .description("Requests answered 503 because the in-flight cap was reached")
                .register(registry);
        Gauge.builder("gateway.requests.in.flight", inFlight, InFlightLimiter::inFlight)
                .description("Requests in progress")
                .register(registry);
        publishOk = publishTimer(registry, "success");
        publishFailed = publishTimer(registry, "failure");
    }

    private static Timer publishTimer(MeterRegistry registry, String outcome) {
        return Timer.builder("gateway.kafka.publish")
                .description("Time until Kafka acknowledged all records of a batch")
                .tag("outcome", outcome)
                .register(registry);
    }

    /** A batch answered 202: its accepted events are in Kafka and its rejections are on record. */
    public void batchStored(int acceptedEvents, List<Rejection> rejections) {
        accepted.increment(acceptedEvents);
        for (Rejection rejection : rejections) {
            rejected.get(rejection.code()).increment();
        }
    }

    public void rateLimited() {
        rateLimited.increment();
    }

    public void overloaded() {
        overloaded.increment();
    }

    public Timer.Sample startPublish() {
        return Timer.start();
    }

    public void publishFinished(Timer.Sample sample, boolean success) {
        sample.stop(success ? publishOk : publishFailed);
    }
}
