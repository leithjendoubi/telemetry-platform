package com.telemetry.gateway.validation;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.telemetry.gateway.ContractFiles;
import com.telemetry.gateway.ContractFiles.InvalidCase;
import com.telemetry.gateway.validation.ValidationOutcome.Checked;
import com.telemetry.gateway.validation.ValidationOutcome.EnvelopeInvalid;
import com.telemetry.gateway.validation.ValidationOutcome.Rejection;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class BatchValidatorTest {

    private static final Instant NOW = Instant.parse("2026-10-14T12:00:00Z");
    private static final Set<String> ALLOWED = Set.of("checkout-api");

    private final BatchValidator validator =
            new BatchValidator(Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofHours(24), Duration.ofMinutes(5));

    @Test
    void freshValidExampleIsAccepted() {
        Checked result = checked(ContractFiles.freshValidBatch(NOW));
        assertThat(result.rejections()).isEmpty();
        assertThat(result.accepted()).hasSize(1);
        assertThat(result.accepted().get(0).index()).isZero();
    }

    /** FR-ING-04: accepted events carry their timestamp in UTC; the request itself is not changed. */
    @Test
    void timestampWithAnOffsetBecomesTheSameInstantInUtc() {
        for (String[] sentAndExpected : new String[][] {
            {"2026-10-14T13:59:30.123+02:00", "2026-10-14T11:59:30.123Z"},
            {"2026-10-14T06:59:30-05:00", "2026-10-14T11:59:30Z"},
            {"2026-10-14T11:59:30.123456+00:00", "2026-10-14T11:59:30.123456Z"},
            {"2026-10-14t11:59:30.5z", "2026-10-14T11:59:30.5Z"}
        }) {
            ObjectNode batch = ContractFiles.freshValidBatch(NOW);
            ((ObjectNode) batch.get("events").get(0)).put("timestamp", sentAndExpected[0]);
            Checked result = checked(batch);
            assertThat(result.accepted()).as(sentAndExpected[0]).hasSize(1);
            assertThat(result.accepted().get(0).event().get("timestamp").asText())
                    .as(sentAndExpected[0])
                    .isEqualTo(sentAndExpected[1]);
            assertThat(batch.get("events").get(0).get("timestamp").asText())
                    .as("the request (202 body, rejected topic) keeps what was sent")
                    .isEqualTo(sentAndExpected[0]);
        }
    }

    @Test
    void utcTimestampIsKeptExactlyAsSent() {
        ObjectNode batch = ContractFiles.freshValidBatch(NOW);
        String sent = batch.get("events").get(0).get("timestamp").asText();
        assertThat(sent).endsWith("Z");
        assertThat(checked(batch).accepted().get(0).event().get("timestamp").asText())
                .isEqualTo(sent);
    }

    static Stream<InvalidCase> invalidCases() {
        return ContractFiles.invalidCases().stream();
    }

    /**
     * Every case of libs/telemetry-contracts/examples/invalid-cases.json.
     * A SCHEMA case that only touches /events/0/... rejects that one event; any other SCHEMA case breaks the
     * envelope, and the whole batch is refused (HTTP 400). SEMANTIC cases reject the event with the case's code.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidCases")
    void everyInvalidCaseIsRejectedWithItsCode(InvalidCase invalidCase) {
        ObjectNode doc = ContractFiles.apply(ContractFiles.freshValidBatch(NOW), invalidCase, NOW);
        ValidationOutcome outcome = validator.validate(doc, ALLOWED);

        if (invalidCase.expect().equals("SCHEMA") && !invalidCase.touchesOnlyEvents()) {
            assertThat(outcome).isInstanceOf(EnvelopeInvalid.class);
            assertThat(((EnvelopeInvalid) outcome).errors()).isNotEmpty();
            return;
        }
        Checked result = (Checked) outcome;
        assertThat(result.accepted()).isEmpty();
        assertThat(result.rejections()).singleElement().satisfies(r -> {
            assertThat(r.index()).isZero();
            assertThat(r.code().name()).isEqualTo(invalidCase.code());
            assertThat(r.eventId()).isEqualTo("0192a3b4-7c10-7d3e-8a1f-5a2b9c4d6e70");
            assertThat(r.message()).isNotBlank();
        });
    }

    @Test
    void duplicateEventIdRejectsTheSecondCopyOnly() {
        ObjectNode batch = batchOf(3);
        String firstId = batch.get("events").get(0).get("eventId").asText();
        ((ObjectNode) batch.get("events").get(2)).put("eventId", firstId.toUpperCase()); // same UUID, other case

        Checked result = checked(batch);
        assertThat(result.accepted())
                .extracting(ValidationOutcome.AcceptedEvent::index)
                .containsExactly(0, 1);
        assertThat(result.rejections()).singleElement().satisfies(r -> {
            assertThat(r.index()).isEqualTo(2);
            assertThat(r.code()).isEqualTo(RejectionCode.DUPLICATE_EVENT_ID);
        });
    }

    @Test
    void duplicateOfARejectedEventIsAccepted() {
        ObjectNode batch = batchOf(2);
        ObjectNode first = (ObjectNode) batch.get("events").get(0);
        first.put("timestamp", ContractFiles.iso(NOW.minus(Duration.ofHours(30))));
        ((ObjectNode) batch.get("events").get(1))
                .put("eventId", first.get("eventId").asText());

        Checked result = checked(batch);
        assertThat(result.rejections())
                .extracting(Rejection::code)
                .containsExactly(RejectionCode.TIMESTAMP_OUT_OF_RANGE);
        assertThat(result.accepted())
                .extracting(ValidationOutcome.AcceptedEvent::index)
                .containsExactly(1);
    }

    @Test
    void serviceOutsideTheAllowListIsRejected() {
        ObjectNode batch = batchOf(2);
        ((ObjectNode) batch.get("events").get(1).get("resource")).put("serviceName", "payment-api");

        Checked result = checked(batch);
        assertThat(result.accepted()).hasSize(1);
        assertThat(result.rejections()).singleElement().satisfies(r -> {
            assertThat(r.index()).isEqualTo(1);
            assertThat(r.code()).isEqualTo(RejectionCode.SERVICE_NOT_PERMITTED);
        });
    }

    @Test
    void partialAcceptanceKeepsGoodEventsAndReportsEachBadOne() {
        ObjectNode batch = batchOf(4);
        ArrayNode events = (ArrayNode) batch.get("events");
        ((ObjectNode) events.get(1).get("metrics")).put("cpuUsagePercent", 150);
        ((ObjectNode) events.get(3).get("metrics")).put("memoryUsedBytes", 9_000_000_000L);

        Checked result = checked(batch);
        assertThat(result.accepted())
                .extracting(ValidationOutcome.AcceptedEvent::index)
                .containsExactly(0, 2);
        assertThat(result.rejections())
                .extracting(Rejection::index, Rejection::code)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(1, RejectionCode.SCHEMA_VIOLATION),
                        org.assertj.core.groups.Tuple.tuple(3, RejectionCode.SEMANTIC_VIOLATION));
        assertThat(result.rejections().get(0).message()).contains("/events/1/metrics/cpuUsagePercent");
    }

    @Test
    void timestampWindowEdgesAreInclusive() {
        assertThat(codeForTimestamp(NOW.minus(Duration.ofHours(24)))).isNull();
        assertThat(codeForTimestamp(NOW.plus(Duration.ofMinutes(5)))).isNull();
        assertThat(codeForTimestamp(NOW.minus(Duration.ofHours(24)).minusMillis(1)))
                .isEqualTo(RejectionCode.TIMESTAMP_OUT_OF_RANGE);
        assertThat(codeForTimestamp(NOW.plus(Duration.ofMinutes(5)).plusMillis(1)))
                .isEqualTo(RejectionCode.TIMESTAMP_OUT_OF_RANGE);
    }

    @Test
    void timestampWithOffsetIsComparedInUtc() {
        ObjectNode batch = batchOf(1);
        // 13:58+02:00 is 11:58Z, two minutes before NOW
        ((ObjectNode) batch.get("events").get(0)).put("timestamp", "2026-10-14T13:58:00+02:00");
        assertThat(checked(batch).rejections()).isEmpty();
    }

    @Test
    void formatsAreChecked() {
        ObjectNode badUuid = batchOf(1);
        ((ObjectNode) badUuid.get("events").get(0)).put("eventId", "not-a-uuid");
        assertThat(checked(badUuid).rejections())
                .extracting(Rejection::code)
                .containsExactly(RejectionCode.SCHEMA_VIOLATION);

        ObjectNode badDate = batchOf(1);
        ((ObjectNode) badDate.get("events").get(0)).put("timestamp", "yesterday");
        assertThat(checked(badDate).rejections())
                .extracting(Rejection::code)
                .containsExactly(RejectionCode.SCHEMA_VIOLATION);

        ObjectNode badBatchId = batchOf(1);
        badBatchId.put("batchId", "123");
        assertThat(validator.validate(badBatchId, ALLOWED)).isInstanceOf(EnvelopeInvalid.class);
    }

    @Test
    void eventThatIsNotAnObjectIsRejectedAlone() {
        ObjectNode batch = batchOf(2);
        ((ArrayNode) batch.get("events"))
                .set(0, ContractFiles.MAPPER.getNodeFactory().numberNode(42));

        Checked result = checked(batch);
        assertThat(result.accepted())
                .extracting(ValidationOutcome.AcceptedEvent::index)
                .containsExactly(1);
        assertThat(result.rejections()).singleElement().satisfies(r -> {
            assertThat(r.code()).isEqualTo(RejectionCode.SCHEMA_VIOLATION);
            assertThat(r.eventId()).isNull();
        });
    }

    // ------------------------------------------------------------------ helpers

    private Checked checked(JsonNode batch) {
        ValidationOutcome outcome = validator.validate(batch, ALLOWED);
        assertThat(outcome).isInstanceOf(Checked.class);
        return (Checked) outcome;
    }

    private RejectionCode codeForTimestamp(Instant timestamp) {
        ObjectNode batch = batchOf(1);
        ((ObjectNode) batch.get("events").get(0)).put("timestamp", ContractFiles.iso(timestamp));
        List<Rejection> rejections = checked(batch).rejections();
        return rejections.isEmpty() ? null : rejections.get(0).code();
    }

    /** Valid batch with n events, each with its own eventId and instanceId. */
    static ObjectNode batchOf(int n) {
        ObjectNode batch = ContractFiles.freshValidBatch(NOW);
        ArrayNode events = (ArrayNode) batch.get("events");
        ObjectNode template = (ObjectNode) events.get(0);
        events.removeAll();
        for (int i = 0; i < n; i++) {
            ObjectNode event = template.deepCopy();
            event.put("eventId", String.format("0192a3b4-7c10-7d3e-8a1f-%012d", i));
            ((ObjectNode) event.get("resource")).put("instanceId", "checkout-api-%02d".formatted(i));
            events.add(event);
        }
        return batch;
    }
}
