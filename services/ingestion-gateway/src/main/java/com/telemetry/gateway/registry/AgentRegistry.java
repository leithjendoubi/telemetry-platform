package com.telemetry.gateway.registry;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.telemetry.gateway.auth.AgentCredential;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/**
 * Agents by API-key hash, with a cache in front of the database (spec 7: "AgentRepository, R2DBC adapter with cache").
 *
 * <ul>
 *   <li>Entries live {@code ttl} (10 s, decided 2026-11-05): a revocation or a new agent takes effect within that time.
 *   <li>"No such agent" is cached too, and the cache is bounded, so a flood of random keys neither reaches the database
 *       for every request nor fills the memory.
 *   <li>Concurrent requests with the same key share one lookup. A failed lookup (database down) is not cached.
 * </ul>
 */
public class AgentRegistry {

    private final AsyncCache<String, Optional<AgentCredential>> cache;
    private final Function<String, Mono<Optional<AgentCredential>>> loader;

    /**
     * @param loader  looks one hash up in the store; empty Optional when no agent has it
     * @param ttl     how long an answer is reused
     * @param maxSize most hashes kept
     * @param ticker  the cache's clock ({@code Ticker.systemTicker()}; tests pass a fake one)
     */
    public AgentRegistry(
            Function<String, Mono<Optional<AgentCredential>>> loader, Duration ttl, long maxSize, Ticker ticker) {
        this.loader = loader;
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(ttl)
                .maximumSize(maxSize)
                .ticker(ticker)
                .buildAsync();
    }

    public Mono<Optional<AgentCredential>> findByKeyHash(String keyHash) {
        // suppressCancel: one client hanging up must not cancel the lookup other requests are waiting for
        return Mono.fromFuture(
                () -> cache.get(keyHash, (hash, executor) -> loader.apply(hash).toFuture()), true);
    }
}
