package com.telemetry.gateway.publish;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.telemetry.gateway.validation.ValidationOutcome.AcceptedEvent;
import com.telemetry.gateway.validation.ValidationOutcome.Rejection;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

/**
 * The gateway's outbound port to Kafka (spec 7, "EventPublisher"; FR-ING-07). Record shapes: libs/telemetry-contracts.
 * <ul>
 *   <li>accepted events to telemetry.raw.v1, key = instanceId (telemetry-raw-event.1.0.json), so one instance's events
 *       stay in order;
 *   <li>rejected events to telemetry.rejected.v1, key = agentId (telemetry-rejected-event.1.0.json), AT-02.
 * </ul>
 *
 * <p>Both kinds are sent together, and the returned Mono completes only when the broker has acknowledged every record
 * (acks=all, set in KafkaConfig). So the gateway answers 202 only when the accepted events are durably stored and the
 * rejections are on record (FR-ING-01); otherwise the agent gets 503 and resends the batch.
 */
public class EventPublisher {

    public static final String RAW_EVENT_TYPE = "TelemetryReceived";
    public static final String REJECTED_EVENT_TYPE = "TelemetryRejected";
    public static final String SCHEMA_VERSION = "1.0";

    private final KafkaSender<String, byte[]> sender;
    private final ObjectMapper mapper;
    private final String rawTopic;
    private final String rejectedTopic;
    private final String producerName;
    private final Clock clock;

    public EventPublisher(
            KafkaSender<String, byte[]> sender,
            ObjectMapper mapper,
            String rawTopic,
            String rejectedTopic,
            String producerName,
            Clock clock) {
        this.sender = sender;
        this.mapper = mapper;
        this.rawTopic = rawTopic;
        this.rejectedTopic = rejectedTopic;
        this.producerName = producerName;
        this.clock = clock;
    }

    /**
     * @param events     the request's events array, as sent: the source of the rejected events' fragments
     * @param receivedAt gateway clock when the request arrived
     */
    public Mono<Void> publish(
            List<AcceptedEvent> accepted,
            List<Rejection> rejections,
            JsonNode events,
            String agentId,
            String batchId,
            Instant receivedAt) {
        String receivedAtText = isoMillis(receivedAt);
        Flux<SenderRecord<String, byte[], String>> records = Flux.concat(
                Flux.fromIterable(accepted)
                        .map(a -> SenderRecord.create(
                                rawRecord(a, agentId, batchId, receivedAtText), "accepted event " + a.index())),
                Flux.fromIterable(rejections)
                        .map(r -> SenderRecord.create(
                                rejectedRecord(r, events.get(r.index()), agentId, batchId, receivedAtText),
                                "rejected event " + r.index())));
        return sender.send(records)
                .doOnNext(result -> {
                    if (result.exception() != null) {
                        throw new PublishException(result.correlationMetadata() + " not stored", result.exception());
                    }
                })
                .then();
    }

    private ProducerRecord<String, byte[]> rawRecord(
            AcceptedEvent accepted, String agentId, String batchId, String receivedAt) {
        ObjectNode value = accepted.event().deepCopy();
        value.put("receivedAt", receivedAt);
        value.put("agentId", agentId);
        value.put("batchId", batchId);
        String key = value.get("resource").get("instanceId").asText();
        return record(rawTopic, key, value, RAW_EVENT_TYPE);
    }

    private ProducerRecord<String, byte[]> rejectedRecord(
            Rejection rejection, JsonNode event, String agentId, String batchId, String receivedAt) {
        ObjectNode value = mapper.createObjectNode();
        value.put("agentId", agentId);
        value.put("batchId", batchId);
        value.put("receivedAt", receivedAt);
        value.put("index", rejection.index());
        value.put("eventId", rejection.eventId()); // null when missing or not a string
        value.put("code", rejection.code().name());
        value.put("message", rejection.message());
        value.set("event", event == null ? NullNode.getInstance() : event.deepCopy());
        return record(rejectedTopic, agentId, value, REJECTED_EVENT_TYPE);
    }

    private ProducerRecord<String, byte[]> record(String topic, String key, ObjectNode value, String eventType) {
        RecordHeaders headers = new RecordHeaders(); // envelope of spec 8.3; traceparent comes with OpenTelemetry (B2)
        headers.add("event-type", bytes(eventType));
        headers.add("schema-version", bytes(SCHEMA_VERSION));
        headers.add("produced-at", bytes(isoMillis(clock.instant())));
        headers.add("producer", bytes(producerName));
        try {
            return new ProducerRecord<>(topic, null, key, mapper.writeValueAsBytes(value), headers);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("a parsed JSON tree can always be written back", e);
        }
    }

    private static String isoMillis(Instant instant) {
        return DateTimeFormatter.ISO_INSTANT.format(instant.truncatedTo(ChronoUnit.MILLIS));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** Kafka did not acknowledge a record; the client gets 503 and retries the batch. */
    public static class PublishException extends RuntimeException {
        public PublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
