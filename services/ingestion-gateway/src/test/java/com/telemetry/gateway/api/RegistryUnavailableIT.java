package com.telemetry.gateway.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
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
