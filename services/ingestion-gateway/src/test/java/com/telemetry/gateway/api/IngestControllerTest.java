package com.telemetry.gateway.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.telemetry.gateway.ContractFiles;
import com.telemetry.gateway.ContractFiles.InvalidCase;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The whole gateway over HTTP, publishing to a real Kafka broker in Docker (Testcontainers).
 * The topic is created by the test, with the 12 partitions of create-topics.sh, because the
 * real broker has auto-creation switched off.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "30s")
@Testcontainers
class IngestControllerTest {

    static final String URL = "/api/v1/telemetry/ingest";
    static final String TOPIC = "telemetry.raw.v1";
    static final String KEY = "dev-agent-checkout-api"; // simulator dev key of agent-checkout-api

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.1");

    static KafkaConsumer<String, byte[]> consumer;
    static JsonSchema rawEventSchema;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("gateway.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @BeforeAll
    static void createTopicAndConsumer() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 12, (short) 1))).all().get();
        }
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "gateway-test");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumer = new KafkaConsumer<>(p);
        consumer.subscribe(List.of(TOPIC));

        try (InputStream in = IngestControllerTest.class.getClassLoader()
                .getResourceAsStream("contracts/schemas/telemetry-raw-event.1.0.json")) {
            rawEventSchema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
                    .getSchema(in, SchemaValidatorsConfig.builder().formatAssertionsEnabled(true).build());
        }
    }

    @AfterAll
    static void closeConsumer() {
        consumer.close();
    }

    @Autowired
    WebTestClient client;

    // ------------------------------------------------------------------ the happy path

    @Test
    void validBatchIsPublishedToKafkaAndAnswered202() throws Exception {
        Instant before = Instant.now();
        ObjectNode batch = freshBatch(1);
        String batchId = batch.get("batchId").asText();
        JsonNode sentEvent = batch.get("events").get(0);

        JsonNode body = post(batch, KEY)
                .header("X-Request-Id", "req-42")
                .exchange()
                .expectStatus().isAccepted()
                .expectHeader().valueEquals("X-Request-Id", "req-42")
                .expectBody(JsonNode.class).returnResult().getResponseBody();
        assertThat(body.get("batchId").asText()).isEqualTo(batchId);
        assertThat(body.get("accepted").asInt()).isEqualTo(1);
        assertThat(body.get("rejected").asInt()).isZero();
        assertThat(body.get("rejections")).isEmpty();

        List<ConsumerRecord<String, byte[]>> records = recordsOfBatch(batchId, 1);
        ConsumerRecord<String, byte[]> record = records.get(0);
        assertThat(record.key()).isEqualTo(sentEvent.get("resource").get("instanceId").asText());

        JsonNode value = ContractFiles.MAPPER.readTree(record.value());
        assertThat(rawEventSchema.validate(value)).as("record matches telemetry-raw-event.1.0.json").isEmpty();
        assertThat(value.get("agentId").asText()).isEqualTo("agent-checkout-api");
        assertThat(value.get("batchId").asText()).isEqualTo(batchId);
        Instant receivedAt = Instant.parse(value.get("receivedAt").asText());
        assertThat(receivedAt).isBetween(before.minusMillis(1), Instant.now());
        // the event itself is passed on unchanged
        ObjectNode withoutAdded = ((ObjectNode) value).deepCopy();
        withoutAdded.remove(List.of("agentId", "batchId", "receivedAt"));
        assertThat(withoutAdded).isEqualTo(sentEvent);

        assertThat(header(record, "event-type")).isEqualTo("TelemetryReceived");
        assertThat(header(record, "schema-version")).isEqualTo("1.0");
        assertThat(header(record, "producer")).isEqualTo("ingestion-gateway/0.1.0");
        assertThat(Instant.parse(header(record, "produced-at"))).isAfterOrEqualTo(receivedAt.minusMillis(1));
    }

    @Test
    void partialBatchPublishesOnlyTheValidEvents() {
        ObjectNode batch = freshBatch(3);
        ((ObjectNode) batch.get("events").get(1).get("metrics")).put("cpuUsagePercent", 101);
        String batchId = batch.get("batchId").asText();

        JsonNode body = post(batch, KEY).exchange()
                .expectStatus().isAccepted()
                .expectBody(JsonNode.class).returnResult().getResponseBody();
        assertThat(body.get("accepted").asInt()).isEqualTo(2);
        assertThat(body.get("rejected").asInt()).isEqualTo(1);
        JsonNode rejection = body.get("rejections").get(0);
        assertThat(rejection.get("index").asInt()).isEqualTo(1);
        assertThat(rejection.get("code").asText()).isEqualTo("SCHEMA_VIOLATION");
        assertThat(rejection.get("eventId").asText()).isEqualTo(batch.get("events").get(1).get("eventId").asText());
        assertThat(rejection.get("message").asText()).contains("cpuUsagePercent");

        List<String> publishedIds = recordsOfBatch(batchId, 2).stream()
                .map(r -> readTree(r.value()).get("eventId").asText()).toList();
        assertThat(publishedIds).containsExactlyInAnyOrder(
                batch.get("events").get(0).get("eventId").asText(),
                batch.get("events").get(2).get("eventId").asText());
    }

    @Test
    void eventsOfOneInstanceShareAPartition() {
        ObjectNode batch = freshBatch(3);
        for (JsonNode event : batch.get("events")) {
            ((ObjectNode) event.get("resource")).put("instanceId", "checkout-api-03");
        }
        post(batch, KEY).exchange().expectStatus().isAccepted();
        List<ConsumerRecord<String, byte[]>> records = recordsOfBatch(batch.get("batchId").asText(), 3);
        assertThat(records).extracting(ConsumerRecord::partition).containsOnly(records.get(0).partition());
    }

    // ------------------------------------------------------------------ the contract's invalid cases, over HTTP

    static Stream<InvalidCase> invalidCases() {
        return ContractFiles.invalidCases().stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidCases")
    void everyInvalidCaseGetsTheRightAnswer(InvalidCase invalidCase) {
        ObjectNode batch = freshBatch(1);
        ObjectNode doc = ContractFiles.apply(batch, invalidCase, Instant.now());
        boolean envelope = invalidCase.expect().equals("SCHEMA") && !invalidCase.touchesOnlyEvents();

        if (envelope) {
            post(doc, KEY).exchange()
                    .expectStatus().isBadRequest()
                    .expectHeader().contentType(MediaType.APPLICATION_PROBLEM_JSON)
                    .expectBody().jsonPath("$.errors").isNotEmpty();
        } else {
            JsonNode body = post(doc, KEY).exchange()
                    .expectStatus().isAccepted()
                    .expectBody(JsonNode.class).returnResult().getResponseBody();
            assertThat(body.get("accepted").asInt()).isZero();
            assertThat(body.get("rejected").asInt()).isEqualTo(1);
            assertThat(body.get("rejections").get(0).get("code").asText()).isEqualTo(invalidCase.code());
        }
    }

    @Test
    void fullyRejectedBatchPublishesNothing() throws Exception {
        long before = recordsInTopic();
        ObjectNode bad = freshBatch(2);
        for (JsonNode event : bad.get("events")) {
            ((ObjectNode) event.get("metrics")).put("memoryUsedBytes", 9_999_999_999L);
        }
        post(bad, KEY).exchange().expectStatus().isAccepted()
                .expectBody().jsonPath("$.accepted").isEqualTo(0).jsonPath("$.rejected").isEqualTo(2);
        // 202 is only sent after Kafka acknowledged, so anything published would already be counted here
        assertThat(recordsInTopic()).isEqualTo(before);

        post(freshBatch(1), KEY).exchange().expectStatus().isAccepted();
        assertThat(recordsInTopic()).as("the counter does see a published event").isEqualTo(before + 1);
    }

    // ------------------------------------------------------------------ whole-request failures

    @Test
    void missingOrWrongApiKeyIs401() {
        client.post().uri(URL).contentType(MediaType.APPLICATION_JSON).bodyValue(freshBatch(1).toString())
                .exchange().expectStatus().isUnauthorized()
                .expectHeader().contentType(MediaType.APPLICATION_PROBLEM_JSON);
        post(freshBatch(1), "dev-agent-nope").exchange().expectStatus().isUnauthorized();
    }

    @Test
    void keyOfAnotherAgentIs403() {
        post(freshBatch(1), "dev-agent-cart-api").exchange().expectStatus().isForbidden();
    }

    @Test
    void wrongContentTypeOrEncodingIs415() {
        client.post().uri(URL).header("X-API-Key", KEY).contentType(MediaType.TEXT_PLAIN)
                .bodyValue(freshBatch(1).toString())
                .exchange().expectStatus().isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE);
        post(freshBatch(1), KEY).header("Content-Encoding", "gzip")
                .exchange().expectStatus().isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE);
    }

    @Test
    void jsonContentTypeWithCharsetIsAccepted() {
        client.post().uri(URL).header("X-API-Key", KEY)
                .contentType(MediaType.parseMediaType("application/json; charset=utf-8"))
                .bodyValue(freshBatch(1).toString())
                .exchange().expectStatus().isAccepted();
    }

    @Test
    void bodyOver256KiBIs413() {
        ObjectNode batch = freshBatch(1);
        batch.put("padding", "x".repeat(262_144));
        post(batch, KEY).exchange().expectStatus().isEqualTo(HttpStatus.PAYLOAD_TOO_LARGE);
    }

    @Test
    void moreThan100EventsIs413And100IsFine() {
        post(freshBatch(101), KEY).exchange().expectStatus().isEqualTo(HttpStatus.PAYLOAD_TOO_LARGE);
        post(freshBatch(100), KEY).exchange().expectStatus().isAccepted()
                .expectBody().jsonPath("$.accepted").isEqualTo(100);
    }

    @Test
    void malformedJsonIs400() {
        for (String body : List.of("{\"schemaVersion\": ", "", "{\"x\": NaN}", "[1,2]")) {
            client.post().uri(URL).header("X-API-Key", KEY).contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(body.getBytes(StandardCharsets.UTF_8))
                    .exchange().expectStatus().isBadRequest();
        }
    }

    @Test
    void healthProbesAnswer() {
        client.get().uri("/health/live").exchange().expectStatus().isOk().expectBody().jsonPath("$.status").isEqualTo("UP");
        client.get().uri("/health/ready").exchange().expectStatus().isOk().expectBody().jsonPath("$.status").isEqualTo("UP");
    }

    // ------------------------------------------------------------------ helpers

    WebTestClient.RequestHeadersSpec<?> post(JsonNode batch, String apiKey) {
        return client.post().uri(URL)
                .header("X-API-Key", apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(batch.toString());
    }

    /** Valid batch of n events with fresh timestamps and unique batchId, eventIds and instanceIds. */
    static ObjectNode freshBatch(int n) {
        ObjectNode batch = ContractFiles.freshValidBatch(Instant.now());
        batch.put("batchId", UUID.randomUUID().toString());
        ArrayNode events = (ArrayNode) batch.get("events");
        ObjectNode template = (ObjectNode) events.get(0);
        events.removeAll();
        for (int i = 0; i < n; i++) {
            ObjectNode event = template.deepCopy();
            event.put("eventId", UUID.randomUUID().toString());
            ((ObjectNode) event.get("resource")).put("instanceId", "checkout-api-%02d".formatted(i % 10));
            events.add(event);
        }
        return batch;
    }

    /** Exactly the records of one batch; fails if fewer than {@code expected} arrive within 15 s. */
    static List<ConsumerRecord<String, byte[]>> recordsOfBatch(String batchId, int expected) {
        List<ConsumerRecord<String, byte[]>> mine = pollUntil(batchId, expected).stream()
                .filter(r -> readTree(r.value()).get("batchId").asText().equals(batchId)).toList();
        assertThat(mine).hasSize(expected);
        return mine;
    }

    /** Everything polled until {@code expected} records of the batch were seen (or 15 s passed). */
    static synchronized List<ConsumerRecord<String, byte[]>> pollUntil(String batchId, int expected) {
        List<ConsumerRecord<String, byte[]>> seen = new ArrayList<>();
        long found = 0;
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (found < expected && System.nanoTime() < deadline) {
            for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(200))) {
                seen.add(r);
                if (readTree(r.value()).get("batchId").asText().equals(batchId)) {
                    found++;
                }
            }
        }
        return seen;
    }

    /** Sum of the end offsets of all partitions = number of records ever written to the topic. */
    static long recordsInTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            for (int p = 0; p < 12; p++) {
                request.put(new TopicPartition(TOPIC, p), OffsetSpec.latest());
            }
            return admin.listOffsets(request).all().get().values().stream().mapToLong(o -> o.offset()).sum();
        }
    }

    static JsonNode readTree(byte[] value) {
        try {
            return ContractFiles.MAPPER.readTree(value);
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static String header(ConsumerRecord<String, byte[]> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

}
