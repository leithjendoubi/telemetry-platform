package com.telemetry.gateway.registry;

import com.telemetry.gateway.auth.AgentCredential;
import com.telemetry.gateway.auth.AgentCredential.AgentStatus;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;

/**
 * Reads one agent from ingestion_db (table agent, spec 8.4) without blocking the event loop (R2DBC, FR-ING-02).
 * Schema: src/main/resources/db/migration (Flyway).
 */
public class R2dbcAgentLookup implements Function<String, Mono<Optional<AgentCredential>>> {

    private static final String SQL = """
            SELECT agent_id, api_key_hash, allowed_services, status, rate_limit_rps, burst
              FROM agent
             WHERE api_key_hash = :hash
            """;

    private final DatabaseClient client;
    private final Duration timeout;

    /** @param timeout an unanswered lookup fails after this time (the request then gets 503) */
    public R2dbcAgentLookup(DatabaseClient client, Duration timeout) {
        this.client = client;
        this.timeout = timeout;
    }

    @Override
    public Mono<Optional<AgentCredential>> apply(String keyHash) {
        return client.sql(SQL)
                .bind("hash", keyHash)
                .map((row, meta) -> new AgentCredential(
                        row.get("agent_id", String.class),
                        row.get("api_key_hash", String.class),
                        Set.of(row.get("allowed_services", String[].class)),
                        AgentStatus.valueOf(row.get("status", String.class)),
                        row.get("rate_limit_rps", Integer.class),
                        row.get("burst", Integer.class)))
                .one()
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .timeout(timeout);
    }
}
