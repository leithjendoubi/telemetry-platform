package com.telemetry.gateway.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.telemetry.gateway.ContractFiles;
import com.telemetry.gateway.ContractFiles.InvalidCase;
import com.telemetry.gateway.TestDatabase;
import com.telemetry.gateway.auth.ApiKeyAuthenticator;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
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
import org.springframework.boot.test.web.server.LocalServerPort;
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
 * The topics are created by the test, with the partitions of create-topics.sh, because the
 * real broker has auto-creation switched off.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "30s")
@Testcontainers
class IngestControllerIT {

    static final String URL = "/api/v1/telemetry/ingest";
    static final String TOPIC = "telemetry.raw.v1";
    static final String REJECTED_TOPIC = "telemetry.rejected.v1";
    static final String KEY = "dev-agent-checkout-api"; // simulator dev key of agent-checkout-api

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.1");

    static KafkaConsumer<String, byte[]> consumer;
    static KafkaConsumer<String, byte[]> rejectedConsumer;
    static JsonSchema rawEventSchema;
    static JsonSchema rejectedEventSchema;

    @DynamicPropertySource
    static void kafkaAndDatabaseProperties(DynamicPropertyRegistry registry) {
        registry.add("gateway.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        TestDatabase.register(registry);
        registry.add("gateway.registry.cache-ttl", () -> "1s"); // so the revocation test need not wait 10 s
    }

    @BeforeAll
    static void createTopicAndConsumer() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 12, (short) 1), new NewTopic(REJECTED_TOPIC, 3, (short) 1)))
                    .all()
                    .get();
        }
        consumer = consumerOf(TOPIC);
        rejectedConsumer = consumerOf(REJECTED_TOPIC);
        rawEventSchema = ContractFiles.schema("telemetry-raw-event.1.0.json");
        rejectedEventSchema = ContractFiles.schema("telemetry-rejected-event.1.0.json");
    }

    static KafkaConsumer<String, byte[]> consumerOf(String topic) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "gateway-test-" + topic);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(p);
        c.subscribe(List.of(topic));
        return c;
    }

    @AfterAll
    static void closeConsumers() {
        consumer.close();
        rejectedConsumer.close();
    }

    @Autowired
    WebTestClient client;

    @LocalServerPort
    int port;

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
                .expectStatus()
                .isAccepted()
                .expectHeader()
                .valueEquals("X-Request-Id", "req-42")
                .expectBody(JsonNode.class)
                .returnResult()
                .getResponseBody();
        assertThat(body.get("batchId").asText()).isEqualTo(batchId);
        assertThat(body.get("accepted").asInt()).isEqualTo(1);
        assertThat(body.get("rejected").asInt()).isZero();
        assertThat(body.get("rejections")).isEmpty();

        List<ConsumerRecord<String, byte[]>> records = recordsOfBatch(batchId, 1);
        ConsumerRecord<String, byte[]> record = records.get(0);
        assertThat(record.key())
                .isEqualTo(sentEvent.get("resource").get("instanceId").asText());

        JsonNode value = ContractFiles.MAPPER.readTree(record.value());
        assertThat(rawEventSchema.validate(value))
                .as("record matches telemetry-raw-event.1.0.json")
                .isEmpty();
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

        JsonNode body = post(batch, KEY)
                .exchange()
                .expectStatus()
                .isAccepted()
                .expectBody(JsonNode.class)
                .returnResult()
                .getResponseBody();
        assertThat(body.get("accepted").asInt()).isEqualTo(2);
        assertThat(body.get("rejected").asInt()).isEqualTo(1);
        JsonNode rejection = body.get("rejections").get(0);
        assertThat(rejection.get("index").asInt()).isEqualTo(1);
        assertThat(rejection.get("code").asText()).isEqualTo("SCHEMA_VIOLATION");
        assertThat(rejection.get("eventId").asText())
                .isEqualTo(batch.get("events").get(1).get("eventId").asText());
        assertThat(rejection.get("message").asText()).contains("cpuUsagePercent");

        List<String> publishedIds = recordsOfBatch(batchId, 2).stream()
                .map(r -> readTree(r.value()).get("eventId").asText())
                .toList();
        assertThat(publishedIds)
                .containsExactlyInAnyOrder(
                        batch.get("events").get(0).get("eventId").asText(),
                        batch.get("events").get(2).get("eventId").asText());
    }

    /**
     * AT-02 (spec 13.6): a batch with valid and invalid events. Valid events go to telemetry.raw.v1, invalid ones are
     * listed with their codes in the 202 body AND published to telemetry.rejected.v1 (FR-ING-07).
     */
    @Test
    void at02MixedBatchListsAndPublishesTheRejectedEvents() throws Exception {
        long rawBefore = recordsInTopic();
        long rejectedBefore = recordsInTopic(REJECTED_TOPIC, 3);
        ObjectNode batch = freshBatch(3);
        ((ObjectNode) batch.get("events").get(1).get("metrics")).put("cpuUsagePercent", 101); // schema
        ((ObjectNode) batch.get("events").get(2).get("metrics")).put("memoryUsedBytes", 9_999_999_999L); // semantic
        String batchId = batch.get("batchId").asText();

        JsonNode body = post(batch, KEY)
                .exchange()
                .expectStatus()
                .isAccepted()
                .expectBody(JsonNode.class)
                .returnResult()
                .getResponseBody();
        assertThat(body.get("accepted").asInt()).isEqualTo(1);
        assertThat(body.get("rejected").asInt()).isEqualTo(2);

        // 202 comes only after Kafka acknowledged every record, so the counts are final here
        assertThat(recordsInTopic() - rawBefore).isEqualTo(1);
        assertThat(recordsInTopic(REJECTED_TOPIC, 3) - rejectedBefore).isEqualTo(2);
        assertThat(readTree(recordsOfBatch(batchId, 1).get(0).value()).get("eventId"))
                .isEqualTo(batch.get("events").get(0).get("eventId"));

        Map<Integer, JsonNode> bodyByIndex = new HashMap<>();
        body.get("rejections").forEach(r -> bodyByIndex.put(r.get("index").asInt(), r));
        assertThat(bodyByIndex.keySet()).containsExactlyInAnyOrder(1, 2);
        assertThat(bodyByIndex.get(1).get("code").asText()).isEqualTo("SCHEMA_VIOLATION");
        assertThat(bodyByIndex.get(2).get("code").asText()).isEqualTo("SEMANTIC_VIOLATION");

        for (ConsumerRecord<String, byte[]> record : rejectedOfBatch(batchId, 2)) {
            JsonNode value = readTree(record.value());
            assertThat(rejectedEventSchema.validate(value))
                    .as("record matches telemetry-rejected-event.1.0.json")
                    .isEmpty();
            assertThat(record.key()).isEqualTo("agent-checkout-api");
            int index = value.get("index").asInt();
            for (String field : List.of("index", "eventId", "code", "message")) {
                assertThat(value.get(field))
                        .as(field + " equals the 202 body")
                        .isEqualTo(bodyByIndex.get(index).get(field));
            }
            assertThat(value.get("event"))
                    .as("the event as sent")
                    .isEqualTo(batch.get("events").get(index));
            assertThat(value.get("agentId").asText()).isEqualTo("agent-checkout-api");
            assertThat(header(record, "event-type")).isEqualTo("TelemetryRejected");
            assertThat(header(record, "schema-version")).isEqualTo("1.0");
            assertThat(header(record, "producer")).isEqualTo("ingestion-gateway/0.1.0");
        }
    }

    @Test
    void eventsOfOneInstanceShareAPartition() {
        ObjectNode batch = freshBatch(3);
        for (JsonNode event : batch.get("events")) {
            ((ObjectNode) event.get("resource")).put("instanceId", "checkout-api-03");
        }
        post(batch, KEY).exchange().expectStatus().isAccepted();
        List<ConsumerRecord<String, byte[]>> records =
                recordsOfBatch(batch.get("batchId").asText(), 3);
        assertThat(records)
                .extracting(ConsumerRecord::partition)
                .containsOnly(records.get(0).partition());
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
            post(doc, KEY)
                    .exchange()
                    .expectStatus()
                    .isBadRequest()
                    .expectHeader()
                    .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                    .expectBody()
                    .jsonPath("$.errors")
                    .isNotEmpty();
        } else {
            JsonNode body = post(doc, KEY)
                    .exchange()
                    .expectStatus()
                    .isAccepted()
                    .expectBody(JsonNode.class)
                    .returnResult()
                    .getResponseBody();
            assertThat(body.get("accepted").asInt()).isZero();
            assertThat(body.get("rejected").asInt()).isEqualTo(1);
            assertThat(body.get("rejections").get(0).get("code").asText()).isEqualTo(invalidCase.code());
        }
    }

    @Test
    void fullyRejectedBatchPublishesNothingRawButRecordsTheRejections() throws Exception {
        long before = recordsInTopic();
        long rejectedBefore = recordsInTopic(REJECTED_TOPIC, 3);
        ObjectNode bad = freshBatch(2);
        for (JsonNode event : bad.get("events")) {
            ((ObjectNode) event.get("metrics")).put("memoryUsedBytes", 9_999_999_999L);
        }
        post(bad, KEY)
                .exchange()
                .expectStatus()
                .isAccepted()
                .expectBody()
                .jsonPath("$.accepted")
                .isEqualTo(0)
                .jsonPath("$.rejected")
                .isEqualTo(2);
        // 202 is only sent after Kafka acknowledged, so anything published would already be counted here
        assertThat(recordsInTopic()).isEqualTo(before);
        assertThat(recordsInTopic(REJECTED_TOPIC, 3)).isEqualTo(rejectedBefore + 2);

        post(freshBatch(1), KEY).exchange().expectStatus().isAccepted();
        assertThat(recordsInTopic())
                .as("the counter does see a published event")
                .isEqualTo(before + 1);
    }

    // ------------------------------------------------------------------ whole-request failures

    /**
     * AT-03 (spec 13.6): missing, wrong and revoked API keys get 401/403 (spec 8.1: 401 missing or invalid, 403
     * revoked) and nothing reaches Kafka. The batch holds a valid and an invalid event, so a skipped check would
     * show on both topics.
     */
    @Test
    void at03MissingWrongAndRevokedKeysPublishNothing() throws Exception {
        String revokedKey = "test-key-" + UUID.randomUUID();
        String revokedAgent = insertAgent(revokedKey, "REVOKED");
        long rawBefore = recordsInTopic();
        long rejectedBefore = recordsInTopic(REJECTED_TOPIC, 3);

        client.post()
                .uri(URL)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(halfValidBatchOf("agent-checkout-api").toString())
                .exchange()
                .expectStatus()
                .isUnauthorized()
                .expectHeader()
                .contentType(MediaType.APPLICATION_PROBLEM_JSON);
        post(halfValidBatchOf("agent-checkout-api"), "   ")
                .exchange()
                .expectStatus()
                .isUnauthorized();
        post(halfValidBatchOf("agent-checkout-api"), "dev-agent-nope")
                .exchange()
                .expectStatus()
                .isUnauthorized();
        // the hash of a key is not a key
        post(halfValidBatchOf("agent-checkout-api"), ApiKeyAuthenticator.sha256Hex(KEY))
                .exchange()
                .expectStatus()
                .isUnauthorized();
        post(halfValidBatchOf(revokedAgent), revokedKey)
                .exchange()
                .expectStatus()
                .isForbidden()
                .expectBody()
                .jsonPath("$.detail")
                .isEqualTo("The API key has been revoked");

        assertThat(recordsInTopic()).as("nothing on telemetry.raw.v1").isEqualTo(rawBefore);
        assertThat(recordsInTopic(REJECTED_TOPIC, 3))
                .as("nothing on telemetry.rejected.v1")
                .isEqualTo(rejectedBefore);
    }

    /**
     * An early answer (here 401, before the body is needed) must reach a client that is still sending its body.
     * Found live on 2026-11-05: about half of such answers arrived as "connection reset", so a revoked agent saw a
     * network error and retried instead of stopping. The race: the client (Python's urllib, i.e. the simulator) sends
     * headers and body in two steps with "Connection: close"; the gateway answered from the headers and closed before
     * the body arrived, and the late body made the client's system abort the connection (WinError 10053). A raw socket
     * that pauses 500 ms between headers and body makes that race certain.
     */
    @Test
    void earlyAnswerReachesAClientStillSendingItsBody() throws Exception {
        byte[] body = halfValidBatchOf("agent-checkout-api").toString().getBytes(StandardCharsets.UTF_8);
        byte[] head = ("POST " + URL + " HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n"
                        + "X-API-Key: dev-agent-nope\r\nConnection: close\r\n" // as Python's urllib sends it
                        + "Content-Length: " + body.length + "\r\n\r\n")
                .getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < 3; i++) {
            try (Socket socket = new Socket("localhost", port)) {
                socket.setSoTimeout(10_000);
                OutputStream out = socket.getOutputStream();
                out.write(head);
                out.flush();
                Thread.sleep(500); // the network is slow, the body comes later
                out.write(body);
                out.flush();
                String statusLine = new BufferedReader(
                                new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII))
                        .readLine();
                assertThat(statusLine).as("attempt " + i).startsWith("HTTP/1.1 401");
            }
        }
    }

    @Test
    void revokingAnAgentTakesEffectWithinTheCacheTtl() throws Exception {
        String key = "test-key-" + UUID.randomUUID();
        String agentId = insertAgent(key, "ACTIVE");
        ObjectNode batch = freshBatch(1);
        ((ObjectNode) batch.get("agent")).put("agentId", agentId);
        post(batch, key).exchange().expectStatus().isAccepted();

        TestDatabase.update("UPDATE agent SET status = 'REVOKED' WHERE agent_id = ?", agentId);
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos(); // cache TTL is 1 s in this test
        HttpStatus last = HttpStatus.ACCEPTED;
        while (last != HttpStatus.FORBIDDEN && System.nanoTime() < deadline) {
            Thread.sleep(200);
            ObjectNode again = freshBatch(1);
            ((ObjectNode) again.get("agent")).put("agentId", agentId);
            last = HttpStatus.valueOf(post(again, key)
                    .exchange()
                    .returnResult(String.class)
                    .getStatus()
                    .value());
        }
        assertThat(last).isEqualTo(HttpStatus.FORBIDDEN);
    }

    /**
     * AT-04 (spec 13.6): an agent over its quota gets 429 with Retry-After, and other agents are not affected.
     * The test agent may send 1 request per second with a burst of 2 (columns rate_limit_rps and burst).
     */
    @Test
    void at04AgentOverItsQuotaGets429OthersAreUnaffected() throws Exception {
        String key = "test-key-" + UUID.randomUUID();
        String limited = insertAgent(key, "ACTIVE", 1, 2);
        // a neighbour with exactly the same limits: a bucket shared between agents would show here
        String neighbourKey = "test-key-" + UUID.randomUUID();
        String neighbour = insertAgent(neighbourKey, "ACTIVE", 1, 2);
        long rawBefore = recordsInTopic();

        post(batchOf(limited), key).exchange().expectStatus().isAccepted();
        post(freshBatch(1), KEY).exchange().expectStatus().isAccepted(); // another agent, in between
        post(batchOf(limited), key).exchange().expectStatus().isAccepted();
        post(batchOf(limited), key)
                .exchange()
                .expectStatus()
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS)
                .expectHeader()
                .valueEquals("Retry-After", "1")
                .expectHeader()
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody()
                .jsonPath("$.detail")
                .isEqualTo("Rate limit of this agent exceeded, retry later");
        for (int i = 0; i < 3; i++) {
            post(freshBatch(1), KEY).exchange().expectStatus().isAccepted(); // the other agent carries on
        }
        post(batchOf(neighbour), neighbourKey).exchange().expectStatus().isAccepted(); // its own full burst of 2
        post(batchOf(neighbour), neighbourKey).exchange().expectStatus().isAccepted();
        assertThat(recordsInTopic() - rawBefore)
                .as("2 batches of the limited agent + 4 of checkout + 2 of the neighbour, none of the refused one")
                .isEqualTo(8);

        Thread.sleep(1100); // one token is back after 1 s at 1 request/s
        post(batchOf(limited), key).exchange().expectStatus().isAccepted();
    }

    /** A valid one-event batch sent as {@code agentId}. */
    static ObjectNode batchOf(String agentId) {
        ObjectNode batch = freshBatch(1);
        ((ObjectNode) batch.get("agent")).put("agentId", agentId);
        return batch;
    }

    /** A new agent for checkout-api with the given key and status; returns its agentId. */
    static String insertAgent(String apiKey, String status) {
        return insertAgent(apiKey, status, 50, 100);
    }

    static String insertAgent(String apiKey, String status, int rateLimitRps, int burst) {
        String agentId = "agent-test-" + UUID.randomUUID().toString().substring(0, 8);
        TestDatabase.update(
                "INSERT INTO agent (agent_id, display_name, api_key_hash, allowed_services, status, rate_limit_rps,"
                        + " burst) VALUES (?, 'test agent', ?, ARRAY['checkout-api'], ?, ?, ?)",
                agentId,
                ApiKeyAuthenticator.sha256Hex(apiKey),
                status,
                rateLimitRps,
                burst);
        return agentId;
    }

    /** One valid and one invalid event, sent as {@code agentId}. */
    static ObjectNode halfValidBatchOf(String agentId) {
        ObjectNode batch = freshBatch(2);
        ((ObjectNode) batch.get("agent")).put("agentId", agentId);
        ((ObjectNode) batch.get("events").get(1).get("metrics")).put("cpuUsagePercent", 101);
        return batch;
    }

    @Test
    void keyOfAnotherAgentIs403() {
        post(freshBatch(1), "dev-agent-cart-api").exchange().expectStatus().isForbidden();
    }

    @Test
    void wrongContentTypeOrEncodingIs415() {
        client.post()
                .uri(URL)
                .header("X-API-Key", KEY)
                .contentType(MediaType.TEXT_PLAIN)
                .bodyValue(freshBatch(1).toString())
                .exchange()
                .expectStatus()
                .isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE);
        post(freshBatch(1), KEY)
                .header("Content-Encoding", "gzip")
                .exchange()
                .expectStatus()
                .isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE);
    }

    @Test
    void jsonContentTypeWithCharsetIsAccepted() {
        client.post()
                .uri(URL)
                .header("X-API-Key", KEY)
                .contentType(MediaType.parseMediaType("application/json; charset=utf-8"))
                .bodyValue(freshBatch(1).toString())
                .exchange()
                .expectStatus()
                .isAccepted();
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
        post(freshBatch(100), KEY)
                .exchange()
                .expectStatus()
                .isAccepted()
                .expectBody()
                .jsonPath("$.accepted")
                .isEqualTo(100);
    }

    @Test
    void malformedJsonIs400() {
        for (String body : List.of("{\"schemaVersion\": ", "", "{\"x\": NaN}", "[1,2]")) {
            client.post()
                    .uri(URL)
                    .header("X-API-Key", KEY)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(body.getBytes(StandardCharsets.UTF_8))
                    .exchange()
                    .expectStatus()
                    .isBadRequest();
        }
    }

    @Test
    void healthProbesAnswer() {
        client.get()
                .uri("/health/live")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.status")
                .isEqualTo("UP");
        client.get()
                .uri("/health/ready")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.status")
                .isEqualTo("UP");
    }

    // ------------------------------------------------------------------ helpers

    WebTestClient.RequestHeadersSpec<?> post(JsonNode batch, String apiKey) {
        return client.post()
                .uri(URL)
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

    /** Exactly the telemetry.raw.v1 records of one batch; fails if fewer than {@code expected} arrive within 15 s. */
    static List<ConsumerRecord<String, byte[]>> recordsOfBatch(String batchId, int expected) {
        return recordsOfBatch(consumer, batchId, expected);
    }

    /** Exactly the telemetry.rejected.v1 records of one batch, in the same way. */
    static List<ConsumerRecord<String, byte[]>> rejectedOfBatch(String batchId, int expected) {
        return recordsOfBatch(rejectedConsumer, batchId, expected);
    }

    static List<ConsumerRecord<String, byte[]>> recordsOfBatch(
            KafkaConsumer<String, byte[]> from, String batchId, int expected) {
        List<ConsumerRecord<String, byte[]>> mine = pollUntil(from, batchId, expected).stream()
                .filter(r -> readTree(r.value()).get("batchId").asText().equals(batchId))
                .toList();
        assertThat(mine).hasSize(expected);
        return mine;
    }

    /** Everything polled until {@code expected} records of the batch were seen (or 15 s passed). */
    static synchronized List<ConsumerRecord<String, byte[]>> pollUntil(
            KafkaConsumer<String, byte[]> from, String batchId, int expected) {
        List<ConsumerRecord<String, byte[]>> seen = new ArrayList<>();
        long found = 0;
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (found < expected && System.nanoTime() < deadline) {
            for (ConsumerRecord<String, byte[]> r : from.poll(Duration.ofMillis(200))) {
                seen.add(r);
                if (readTree(r.value()).get("batchId").asText().equals(batchId)) {
                    found++;
                }
            }
        }
        return seen;
    }

    /** Sum of the end offsets of all partitions = number of records ever written to telemetry.raw.v1. */
    static long recordsInTopic() throws Exception {
        return recordsInTopic(TOPIC, 12);
    }

    static long recordsInTopic(String topic, int partitions) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            for (int p = 0; p < partitions; p++) {
                request.put(new TopicPartition(topic, p), OffsetSpec.latest());
            }
            return admin.listOffsets(request).all().get().values().stream()
                    .mapToLong(o -> o.offset())
                    .sum();
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
