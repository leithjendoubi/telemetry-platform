package com.telemetry.gateway.ratelimit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.telemetry.gateway.auth.Agent;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.ConsumptionProbe;
import io.github.bucket4j.TimeMeter;
import java.time.Duration;

/**
 * Per-agent token bucket (FR-ING-06, AT-04; spec 7 "RateLimitPolicy"). Each agent has its own bucket of {@code burst}
 * tokens refilled at {@code rateLimitRps} per second (table agent, defaults 50 and 100). A request takes one token;
 * an empty bucket means 429 with Retry-After = time until the next token. Other agents are not affected.
 *
 * <p>Buckets live in memory of this gateway instance, keyed by agent and its limits, so a changed limit takes effect
 * with the next registry refresh (10 s). Idle buckets are dropped after {@code idleExpiry} (a new bucket starts full,
 * which only ever favours the agent), and at most {@code maxAgents} are kept. Bucket4j's local buckets are lock-free,
 * so the event loop never blocks (FR-ING-02).
 */
public class AgentRateLimiter {

    /** Outcome for one request. */
    public sealed interface Decision {

        record Allowed() implements Decision {}

        /** Over the limit: answer 429 with this Retry-After (whole seconds, at least 1). */
        record Limited(long retryAfterSeconds) implements Decision {}
    }

    private record BucketKey(String agentId, int rateLimitRps, int burst) {}

    private final Cache<BucketKey, Bucket> buckets;
    private final TimeMeter timeMeter;

    /** @param timeMeter the buckets' clock ({@code TimeMeter.SYSTEM_NANOTIME}; tests pass a fake one) */
    public AgentRateLimiter(Duration idleExpiry, long maxAgents, TimeMeter timeMeter) {
        this.buckets = Caffeine.newBuilder()
                .expireAfterAccess(idleExpiry)
                .maximumSize(maxAgents)
                .build();
        this.timeMeter = timeMeter;
    }

    public Decision tryAcquire(Agent agent) {
        Bucket bucket =
                buckets.get(new BucketKey(agent.agentId(), agent.rateLimitRps(), agent.burst()), this::newBucket);
        ConsumptionProbe probe = bucket.tryConsumeAndReturnRemaining(1);
        if (probe.isConsumed()) {
            return new Decision.Allowed();
        }
        long nanos = probe.getNanosToWaitForRefill();
        long seconds = (nanos + 999_999_999L) / 1_000_000_000L; // round up: never invite the agent back too early
        return new Decision.Limited(Math.max(1, seconds));
    }

    private Bucket newBucket(BucketKey key) {
        Bandwidth limit = Bandwidth.builder()
                .capacity(key.burst())
                .refillGreedy(key.rateLimitRps(), Duration.ofSeconds(1))
                .build();
        return Bucket.builder()
                .addLimit(limit)
                .withCustomTimePrecision(timeMeter)
                .build();
    }
}
