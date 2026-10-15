package com.telemetry.gateway.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Spec 6.8: when Kafka is unavailable the gateway answers 503 with Retry-After, so agents retry,
 * and never 202 (nothing may be acknowledged that Kafka did not store). Nothing listens on port 1.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "gateway.kafka.bootstrap-servers=localhost:1",
        "gateway.kafka.producer.max.block.ms=1000",
        "gateway.kafka.producer.request.timeout.ms=1000",
        "gateway.kafka.producer.delivery.timeout.ms=2000"})
@AutoConfigureWebTestClient(timeout = "30s")
class KafkaUnavailableTest {

    @Autowired
    WebTestClient client;

    @Test
    void brokerDownGives503WithRetryAfter() {
        client.post().uri(IngestControllerTest.URL)
                .header("X-API-Key", IngestControllerTest.KEY)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(IngestControllerTest.freshBatch(1).toString())
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
                .expectHeader().valueEquals("Retry-After", "5")
                .expectHeader().contentType(MediaType.APPLICATION_PROBLEM_JSON);
    }

    @Test
    void fullyRejectedBatchStillGets202WithoutKafka() {
        var batch = IngestControllerTest.freshBatch(1);
        ((com.fasterxml.jackson.databind.node.ObjectNode) batch.get("events").get(0).get("metrics"))
                .put("memoryUsedBytes", 9_999_999_999L);
        client.post().uri(IngestControllerTest.URL)
                .header("X-API-Key", IngestControllerTest.KEY)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(batch.toString())
                .exchange()
                .expectStatus().isAccepted()
                .expectBody().jsonPath("$.accepted").isEqualTo(0);
    }
}
