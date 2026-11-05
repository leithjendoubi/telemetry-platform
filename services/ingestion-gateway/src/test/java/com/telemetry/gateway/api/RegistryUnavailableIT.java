package com.telemetry.gateway.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * When the agent registry (ingestion_db) cannot answer, a key cannot be checked: the agent gets 503 with Retry-After
 * and resends later. Never 401 (that would tell a valid agent its key is wrong) and never a pass. Nothing listens on
 * port 1; Flyway is off because there is no database to migrate.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "spring.r2dbc.url=r2dbc:postgresql://localhost:1/ingestion_db",
            "spring.flyway.enabled=false",
            "gateway.kafka.bootstrap-servers=localhost:1"
        })
@AutoConfigureWebTestClient(timeout = "30s")
@ExtendWith(OutputCaptureExtension.class)
class RegistryUnavailableIT {

    @Autowired
    WebTestClient client;

    @Test
    void databaseDownGives503WithRetryAfter() {
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
                .expectBody()
                .jsonPath("$.detail")
                .isEqualTo("The agent registry is unavailable, retry later");
    }

    /** Spec 12.1, FR-ING-09: logs are JSON on stdout with service and version (ECS); the WARN above is one. */
    @Test
    void logLinesAreJsonWithServiceAndVersion(CapturedOutput output) throws Exception {
        databaseDownGives503WithRetryAfter();
        String line = output.getOut()
                .lines()
                .filter(l -> l.contains("agent registry unavailable"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no log line about the unavailable registry"));
        JsonNode json = new ObjectMapper().readTree(line);
        assertThat(json.at("/log/level").asText()).isEqualTo("WARN"); // Boot 3.5 writes ECS as nested objects
        assertThat(json.at("/service/name").asText()).isEqualTo("ingestion-gateway");
        assertThat(json.at("/service/version").asText()).isEqualTo("0.1.0");
        assertThat(json.path("@timestamp").asText()).isNotBlank();
        assertThat(json.path("message").asText()).startsWith("agent registry unavailable");
    }

    @Test
    void missingKeyIsStill401WithoutAskingTheDatabase() {
        client.post()
                .uri(IngestControllerIT.URL)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(IngestControllerIT.freshBatch(1).toString())
                .exchange()
                .expectStatus()
                .isUnauthorized();
    }
}
