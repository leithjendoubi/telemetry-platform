package com.telemetry.analytics.ingest;

import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.PathType;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

/**
 * Turns one telemetry.scored.v1 value into one {@link TelemetryRow}.
 * The value is first checked against libs/telemetry-contracts (telemetry-scored-event.1.0.json);
 * anything that does not fit, or does not fit the column types, is a {@link PoisonRecordException}.
 *
 * <p>Thread-safe.
 */
public class ScoredEventParser {

    public static final String SCHEMA_RESOURCE = "contracts/schemas/telemetry-scored-event.1.0.json";

    private final ObjectMapper mapper;
    private final JsonSchema schema;

    public ScoredEventParser(ObjectMapper mapper) {
        this.mapper = mapper;
        SchemaValidatorsConfig config = SchemaValidatorsConfig.builder()
                .formatAssertionsEnabled(true)
                .pathType(PathType.JSON_POINTER)
                .build();
        try (InputStream in = ScoredEventParser.class.getClassLoader().getResourceAsStream(SCHEMA_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException(SCHEMA_RESOURCE + " is not on the classpath (see pom.xml resources)");
            }
            this.schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(in, config);
        } catch (IOException e) {
            throw new IllegalStateException("cannot read " + SCHEMA_RESOURCE, e);
        }
        this.schema.initializeValidators();
    }

    public TelemetryRow parse(byte[] value) {
        if (value == null || value.length == 0) {
            throw new PoisonRecordException("empty record value");
        }
        JsonNode e;
        try {
            e = mapper.readTree(value);
        } catch (IOException ex) {
            throw new PoisonRecordException("not JSON: " + ex.getMessage());
        }
        List<ValidationMessage> errors = schema.validate(e).stream()
                .sorted(Comparator.comparing(m -> m.getInstanceLocation().toString())).toList();
        if (!errors.isEmpty()) {
            throw new PoisonRecordException("not a telemetry-scored-event 1.0 record: " + errors.get(0).getMessage());
        }

        JsonNode r = e.get("resource");
        JsonNode m = e.get("metrics");
        JsonNode p = e.get("prediction");
        return new TelemetryRow(
                time(e, "timestamp"),
                UUID.fromString(e.get("eventId").asText()),
                r.get("instanceId").asText(),
                r.get("serviceName").asText(),
                r.get("environment").asText(),
                r.get("region").asText(),
                time(e, "receivedAt"),
                time(e, "scoredAt"),
                m.get("cpuUsagePercent").doubleValue(),
                asLong(m, "memoryUsedBytes"),
                asLong(m, "memoryLimitBytes"),
                m.get("networkRxBytesPerSec").doubleValue(),
                m.get("networkTxBytesPerSec").doubleValue(),
                m.get("diskReadBytesPerSec").doubleValue(),
                m.get("diskWriteBytesPerSec").doubleValue(),
                asInt(m, "activeThreadCount"),
                m.get("httpRequestsPerSec").doubleValue(),
                m.get("httpErrorRatePercent").doubleValue(),
                m.get("httpLatencyP95Ms").doubleValue(),
                m.get("gcPauseMsPerMin").doubleValue(),
                p.get("probability").doubleValue(),
                p.get("isAnomaly").booleanValue(),
                p.get("warmup").booleanValue(),
                p.get("modelVersion").asText());
    }

    private static OffsetDateTime time(JsonNode node, String field) {
        try {
            return OffsetDateTime.parse(node.get(field).asText());
        } catch (DateTimeParseException ex) {
            throw new PoisonRecordException(field + " cannot be parsed: " + node.get(field).asText());
        }
    }

    // JSON Schema "integer" has no upper bound; the columns do (bigint, integer)
    private static long asLong(JsonNode node, String field) {
        if (!node.get(field).canConvertToLong()) {
            throw new PoisonRecordException(field + " does not fit a bigint column");
        }
        return node.get(field).longValue();
    }

    private static int asInt(JsonNode node, String field) {
        if (!node.get(field).canConvertToInt()) {
            throw new PoisonRecordException(field + " does not fit an integer column");
        }
        return node.get(field).intValue();
    }

    /** A record that can never be stored. It goes to the dead-letter topic. */
    public static class PoisonRecordException extends RuntimeException {
        public PoisonRecordException(String message) {
            super(message.length() > 500 ? message.substring(0, 500) : message);
        }
    }
}
