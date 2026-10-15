package com.telemetry.gateway.validation;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonNodePath;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.PathType;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import com.telemetry.gateway.validation.ValidationOutcome.AcceptedEvent;
import com.telemetry.gateway.validation.ValidationOutcome.Checked;
import com.telemetry.gateway.validation.ValidationOutcome.EnvelopeInvalid;
import com.telemetry.gateway.validation.ValidationOutcome.Rejection;

/**
 * Validates one ingest batch (FR-ING-03) in two layers:
 * <ol>
 *   <li>the JSON Schema of libs/telemetry-contracts (draft 2020-12, formats such as uuid and date-time checked);</li>
 *   <li>the per-event rules JSON Schema cannot express (spec 8.1): allowed service, timestamp window,
 *       memoryUsed &lt;= memoryLimit, no duplicate eventId inside the batch.</li>
 * </ol>
 * A schema error inside {@code events[i]} only rejects that event. Any other schema error rejects the whole batch.
 * Each rejected event gets exactly one code: the first rule it breaks, in the order of the list above.
 *
 * <p>Thread-safe: one instance serves all requests.
 */
public class BatchValidator {

    public static final String SCHEMA_RESOURCE = "contracts/schemas/telemetry-batch.1.0.json";
    private static final int MAX_MESSAGES_PER_EVENT = 5;

    private final JsonSchema schema;
    private final Clock clock;
    private final Duration maxEventAge;
    private final Duration maxClockSkew;

    public BatchValidator(Clock clock, Duration maxEventAge, Duration maxClockSkew) {
        this.clock = clock;
        this.maxEventAge = maxEventAge;
        this.maxClockSkew = maxClockSkew;
        SchemaValidatorsConfig config = SchemaValidatorsConfig.builder()
                .formatAssertionsEnabled(true)   // in draft 2020-12 "format" is only a hint unless switched on
                .pathType(PathType.JSON_POINTER) // error messages say /events/0/metrics/... like the contract tests
                .build();
        try (InputStream in = BatchValidator.class.getClassLoader().getResourceAsStream(SCHEMA_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException(SCHEMA_RESOURCE + " is not on the classpath (see pom.xml resources)");
            }
            this.schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(in, config);
        } catch (IOException e) {
            throw new IllegalStateException("cannot read " + SCHEMA_RESOURCE, e);
        }
        this.schema.initializeValidators(); // do the one-time work now, not during the first request
    }

    public ValidationOutcome validate(JsonNode batch, Set<String> allowedServices) {
        // ---- layer 1: JSON Schema, errors sorted into "envelope" and "event i"
        List<String> envelopeErrors = new ArrayList<>();
        Map<Integer, List<String>> eventErrors = new TreeMap<>();
        for (ValidationMessage message : schema.validate(batch)) {
            Integer eventIndex = eventIndexOf(message.getInstanceLocation());
            if (eventIndex == null) {
                envelopeErrors.add(message.getMessage());
            } else {
                eventErrors.computeIfAbsent(eventIndex, i -> new ArrayList<>()).add(message.getMessage());
            }
        }
        if (!envelopeErrors.isEmpty()) {
            envelopeErrors.sort(null);
            return new EnvelopeInvalid(envelopeErrors);
        }

        // ---- layer 2: semantic rules, one event at a time
        Instant now = clock.instant();
        Instant oldest = now.minus(maxEventAge);
        Instant newest = now.plus(maxClockSkew);
        List<AcceptedEvent> accepted = new ArrayList<>();
        List<Rejection> rejections = new ArrayList<>();
        Set<String> acceptedIds = new HashSet<>();

        JsonNode events = batch.get("events");
        for (int i = 0; i < events.size(); i++) {
            JsonNode event = events.get(i);
            String eventId = event.path("eventId").isTextual() ? event.get("eventId").asText() : null;

            List<String> errors = eventErrors.get(i);
            if (errors != null) {
                errors.sort(null);
                String text = String.join("; ", errors.subList(0, Math.min(errors.size(), MAX_MESSAGES_PER_EVENT)));
                rejections.add(new Rejection(i, eventId, RejectionCode.SCHEMA_VIOLATION, text));
                continue;
            }

            String service = event.get("resource").get("serviceName").asText();
            if (!allowedServices.contains(service)) {
                rejections.add(new Rejection(i, eventId, RejectionCode.SERVICE_NOT_PERMITTED,
                        "serviceName '" + service + "' is not in this agent's allow-list"));
                continue;
            }

            Instant timestamp;
            try {
                timestamp = OffsetDateTime.parse(event.get("timestamp").asText()).toInstant();
            } catch (DateTimeParseException e) {
                // valid RFC 3339 that Java cannot read, e.g. a leap second "23:59:60"
                rejections.add(new Rejection(i, eventId, RejectionCode.SCHEMA_VIOLATION,
                        "/events/" + i + "/timestamp: cannot be parsed as an RFC 3339 date-time"));
                continue;
            }
            if (timestamp.isBefore(oldest) || timestamp.isAfter(newest)) {
                rejections.add(new Rejection(i, eventId, RejectionCode.TIMESTAMP_OUT_OF_RANGE,
                        "timestamp " + event.get("timestamp").asText() + " is outside [now - " + maxEventAge
                                + ", now + " + maxClockSkew + "], now = " + now));
                continue;
            }

            JsonNode metrics = event.get("metrics");
            BigDecimal used = metrics.get("memoryUsedBytes").decimalValue();
            BigDecimal limit = metrics.get("memoryLimitBytes").decimalValue();
            if (used.compareTo(limit) > 0) {
                rejections.add(new Rejection(i, eventId, RejectionCode.SEMANTIC_VIOLATION,
                        "memoryUsedBytes (" + used + ") is greater than memoryLimitBytes (" + limit + ")"));
                continue;
            }

            // UUIDs are compared case-insensitively; the first accepted copy wins
            if (!acceptedIds.add(eventId.toLowerCase(Locale.ROOT))) {
                rejections.add(new Rejection(i, eventId, RejectionCode.DUPLICATE_EVENT_ID,
                        "eventId already used by an earlier event of this batch"));
                continue;
            }

            accepted.add(new AcceptedEvent(i, (ObjectNode) event));
        }
        return new Checked(accepted, rejections);
    }

    /** i when the error is inside /events/i, otherwise null (an envelope error, including /events itself). */
    private static Integer eventIndexOf(JsonNodePath location) {
        if (location.getNameCount() >= 2
                && "events".equals(location.getElement(0))
                && location.getElement(1) instanceof Integer index) {
            return index;
        }
        return null;
    }
}
