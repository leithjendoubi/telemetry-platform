package com.telemetry.gateway.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.telemetry.gateway.TestDatabase;
import com.telemetry.gateway.ratelimit.InFlightLimiter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * The global in-flight cap (FR-ING-06, spec 6.8), here 1 instead of 2,000. A raw socket sends a valid request's
 * headers and holds the body back, so that request stays in progress; meanwhile another request gets 503 with
 * Retry-After. When the held request ends, its slot is free again. Kafka is unreachable on purpose: the held request
 * may end in any answer, the test only watches the slot.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "gateway.max-in-flight=1",
            "gateway.kafka.bootstrap-servers=localhost:1",
            "gateway.kafka.producer.max.block.ms=500",
            "gateway.kafka.producer.request.timeout.ms=500",
            "gateway.kafka.producer.delivery.timeout.ms=1000"
        })
@AutoConfigureWebTestClient(timeout = "30s")
class InFlightLimitIT {

    static final String BUSY = "Too many requests in progress, retry later";

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        TestDatabase.register(registry);
    }

    @Autowired
    WebTestClient client;

    @LocalServerPort
    int port;

    @Autowired
    InFlightLimiter limiter;

    @Test
    void requestsBeyondTheCapGet503AndTheSlotIsFreedAfterwards() throws Exception {
        byte[] body = IngestControllerIT.freshBatch(1).toString().getBytes(StandardCharsets.UTF_8);
        try (Socket held = new Socket("localhost", port)) {
            held.setSoTimeout(15_000);
            OutputStream out = held.getOutputStream();
            out.write(("POST " + IngestControllerIT.URL + " HTTP/1.1\r\nHost: localhost\r\n"
                            + "Content-Type: application/json\r\nX-API-Key: " + IngestControllerIT.KEY + "\r\n"
                            + "Content-Length: " + body.length + "\r\n\r\n")
                    .getBytes(StandardCharsets.UTF_8));
            out.flush(); // headers only: the gateway now waits for the body, holding the only slot

            // generous: the first request of a fresh JVM can take seconds to reach the controller (class loading)
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (limiter.inFlight() < 1 && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            assertThat(limiter.inFlight())
                    .as("the held request occupies the slot")
                    .isEqualTo(1);
            assertThat(secondRequestDetail()).as("a request beyond the cap").isEqualTo(BUSY);

            out.write(body); // the held request goes on and ends
            out.flush();
            String statusLine = new BufferedReader(
                            new InputStreamReader(held.getInputStream(), StandardCharsets.US_ASCII))
                    .readLine();
            assertThat(statusLine).startsWith("HTTP/1.1 ");
        }
        assertThat(secondRequestDetail()).as("the slot is free again").isNotEqualTo(BUSY);
    }

    private String secondRequestDetail() {
        var result = client.post()
                .uri(IngestControllerIT.URL)
                .header("X-API-Key", "dev-agent-nope") // answered quickly: 401, or 503 when the cap is reached
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(IngestControllerIT.freshBatch(1).toString())
                .exchange()
                .expectBody(JsonNode.class)
                .returnResult();
        JsonNode body = result.getResponseBody();
        if (result.getStatus().value() == 503) {
            assertThat(result.getResponseHeaders().getFirst("Retry-After")).isEqualTo("5");
        }
        return body == null || body.get("detail") == null
                ? ""
                : body.get("detail").asText();
    }
}
