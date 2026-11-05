package com.telemetry.gateway.registry;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.benmanes.caffeine.cache.Ticker;
import com.telemetry.gateway.auth.AgentCredential;
import com.telemetry.gateway.auth.AgentCredential.AgentStatus;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

/** The cache in front of ingestion_db, with a fake store and a fake clock (no database, no waiting). */
class AgentRegistryTest {

    private static final Duration TTL = Duration.ofSeconds(10);
    private static final String HASH = "a".repeat(64);

    private final AtomicLong nanos = new AtomicLong();
    private final Ticker ticker = nanos::get;
    private final AtomicInteger loads = new AtomicInteger();

    private static Optional<AgentCredential> agent(AgentStatus status) {
        return Optional.of(new AgentCredential("agent-a", HASH, Set.of("svc"), status, 50, 100));
    }

    /** A store that answers with the given results in turn and counts how often it is asked. */
    private AgentRegistry registryAnswering(Deque<Mono<Optional<AgentCredential>>> answers) {
        Function<String, Mono<Optional<AgentCredential>>> loader = hash -> {
            loads.incrementAndGet();
            return answers.isEmpty() ? Mono.just(Optional.empty()) : answers.poll();
        };
        return new AgentRegistry(loader, TTL, 1000, ticker);
    }

    private void advance(Duration d) {
        nanos.addAndGet(d.toNanos());
    }

    @Test
    void answersAreReusedWithinTheTtlIncludingUnknownKeys() {
        AgentRegistry registry = registryAnswering(new ArrayDeque<>());
        assertThat(registry.findByKeyHash(HASH).block()).isEmpty();
        advance(Duration.ofSeconds(9));
        assertThat(registry.findByKeyHash(HASH).block()).isEmpty();
        assertThat(loads)
                .as("a flood of the same unknown key reaches the store once")
                .hasValue(1);
    }

    @Test
    void aRevocationIsSeenAfterTheTtl() {
        Deque<Mono<Optional<AgentCredential>>> answers = new ArrayDeque<>();
        answers.add(Mono.just(agent(AgentStatus.ACTIVE)));
        answers.add(Mono.just(agent(AgentStatus.REVOKED)));
        AgentRegistry registry = registryAnswering(answers);

        assertThat(registry.findByKeyHash(HASH).block())
                .get()
                .extracting(AgentCredential::status)
                .isEqualTo(AgentStatus.ACTIVE);
        advance(Duration.ofSeconds(11));
        assertThat(registry.findByKeyHash(HASH).block())
                .get()
                .extracting(AgentCredential::status)
                .isEqualTo(AgentStatus.REVOKED);
        assertThat(loads).hasValue(2);
    }

    @Test
    void aFailedLookupIsNotCached() {
        Deque<Mono<Optional<AgentCredential>>> answers = new ArrayDeque<>();
        answers.add(Mono.error(new IllegalStateException("database down")));
        answers.add(Mono.just(agent(AgentStatus.ACTIVE)));
        AgentRegistry registry = registryAnswering(answers);

        StepVerifier.create(registry.findByKeyHash(HASH))
                .expectErrorMessage("database down")
                .verify(Duration.ofSeconds(5));
        assertThat(registry.findByKeyHash(HASH).block()).isPresent();
        assertThat(loads).hasValue(2);
    }

    @Test
    void concurrentRequestsShareOneLookupAndACancelDoesNotBreakIt() {
        Sinks.One<Optional<AgentCredential>> slowDatabase = Sinks.one();
        Deque<Mono<Optional<AgentCredential>>> answers = new ArrayDeque<>();
        answers.add(slowDatabase.asMono());
        AgentRegistry registry = registryAnswering(answers);

        Mono<Optional<AgentCredential>> first = registry.findByKeyHash(HASH);
        Mono<Optional<AgentCredential>> second = registry.findByKeyHash(HASH);
        // the first client hangs up while the lookup is still running
        StepVerifier.create(first).thenCancel().verify(Duration.ofSeconds(5));
        StepVerifier.create(second)
                .then(() -> slowDatabase.tryEmitValue(agent(AgentStatus.ACTIVE)))
                .expectNextMatches(Optional::isPresent)
                .verifyComplete();
        assertThat(loads).hasValue(1);
    }
}
