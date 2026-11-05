package com.telemetry.gateway.api;

import com.telemetry.gateway.TestDatabase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Spec 6.8: when Kafka is unavailable the gateway answers 503 with Retry-After, so agents retry,
 * and never 202 (nothing may be acknowledged that Kafka did not store). Nothing listens on port 1.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "gateway.kafka.bootstrap-servers=localhost:1",
            "gateway.kafka.producer.max.block.ms=1000",
            "gateway.kafka.producer.request.timeout.ms=1000",
            "gateway.kafka.producer.delivery.timeout.ms=2000"
        })
@AutoConfigureWebTestClient(timeout = "30s")
class KafkaUnavailableIT {

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        TestDatabase.register(registry); // the agent check comes first and needs the registry
    }

    @Autowired
    WebTestClient client;

    @Test
    void brokerDownGives503WithRetryAfter() {
        client.post()
                .uri(IngestControllerIT.URL)
                .header("X-API-Key", IngestControllerIT.KEY)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(IngestControllerIT.freshBatch(1).toString())
                .exchange()
                .expectStatus()
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
                .expectHeader()
                .valueEquals("Retry-After", "5")
                .expectHeader()
                .contentType(MediaType.APPLICATION_PROBLEM_JSON);
    }

    /**
     * A fully rejected batch also needs Kafka: its rejections must be on telemetry.rejected.v1 before the 202
     * (AT-02, FR-ING-01). With the broker down the agent gets 503 and resends; once Kafka is back it gets 202 with
     * accepted 0 and stops (spec 8.1). ADR-0007.
     */
    @Test
    void fullyRejectedBatchAlsoGets503WithoutKafka() {
        var batch = IngestControllerIT.freshBatch(1);
        ((com.fasterxml.jackson.databind.node.ObjectNode)
                        batch.get("events").get(0).get("metrics"))
                .put("memoryUsedBytes", 9_999_999_999L);
        client.post()
                .uri(IngestControllerIT.URL)
                .header("X-API-Key", IngestControllerIT.KEY)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(batch.toString())
                .exchange()
                .expectStatus()
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
                .expectHeader()
                .valueEquals("Retry-After", "5");
    }
}
