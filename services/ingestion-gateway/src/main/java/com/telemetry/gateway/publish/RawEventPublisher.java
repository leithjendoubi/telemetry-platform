package com.telemetry.gateway.publish;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.telemetry.gateway.validation.ValidationOutcome.AcceptedEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

/**
 * Publishes accepted events to telemetry.raw.v1 (FR-ING-07). Record shape: libs/telemetry-contracts,
 * schemas/telemetry-raw-event.1.0.json. Key = instanceId, so one instance's events stay in order.
 *
 * <p>The returned Mono completes only when the broker has acknowledged every record (acks=all, set in
 * KafkaConfig), so the gateway answers 202 only for data that is durably stored (FR-ING-01).
 */
public class RawEventPublisher {

    public static final String EVENT_TYPE = "TelemetryReceived";
    public static final String SCHEMA_VERSION = "1.0";

    private final KafkaSender<String, byte[]> sender;
    private final ObjectMapper mapper;
    private final String topic;
    private final String producerName;
    private final Clock clock;

    public RawEventPublisher(KafkaSender<String, byte[]> sender, ObjectMapper mapper, String topic,
                             String producerName, Clock clock) {
        this.sender = sender;
        this.mapper = mapper;
        this.topic = topic;
        this.producerName = producerName;
        this.clock = clock;
    }

    public Mono<Void> publish(List<AcceptedEvent> events, String agentId, String batchId, Instant receivedAt) {
        String receivedAtText = isoMillis(receivedAt);
        Flux<SenderRecord<String, byte[], Integer>> records = Flux.fromIterable(events)
                .map(accepted -> SenderRecord.create(toRecord(accepted, agentId, batchId, receivedAtText), accepted.index()));
        return sender.send(records)
                .doOnNext(result -> {
                    if (result.exception() != null) {
                        throw new PublishException("event " + result.correlationMetadata() + " not stored", result.exception());
                    }
                })
                .then();
    }

    private ProducerRecord<String, byte[]> toRecord(AcceptedEvent accepted, String agentId, String batchId, String receivedAt) {
        ObjectNode value = accepted.event().deepCopy();
        value.put("receivedAt", receivedAt);
        value.put("agentId", agentId);
        value.put("batchId", batchId);
        String key = value.get("resource").get("instanceId").asText();

        RecordHeaders headers = new RecordHeaders(); // envelope of spec 8.3; traceparent comes with OpenTelemetry (week 5)
        headers.add("event-type", bytes(EVENT_TYPE));
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

    /** Kafka did not acknowledge an event; the client gets 503 and retries the batch. */
    public static class PublishException extends RuntimeException {
        public PublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
