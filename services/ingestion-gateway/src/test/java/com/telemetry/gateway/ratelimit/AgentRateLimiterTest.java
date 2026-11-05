package com.telemetry.gateway.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import com.telemetry.gateway.auth.Agent;
import com.telemetry.gateway.ratelimit.AgentRateLimiter.Decision;
import io.github.bucket4j.TimeMeter;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Token buckets per agent (FR-ING-06, AT-04), with a fake clock: no waiting. */
class AgentRateLimiterTest {

    private final AtomicLong nanos = new AtomicLong();
    private final TimeMeter clock = new TimeMeter() {
        @Override
        public long currentTimeNanos() {
            return nanos.get();
        }

        @Override
        public boolean isWallClockBased() {
            return false;
        }
    };
    private final AgentRateLimiter limiter = new AgentRateLimiter(Duration.ofMinutes(10), 1000, clock);

    private static Agent agent(String id, int rps, int burst) {
        return new Agent(id, Set.of("svc"), rps, burst);
    }

    private void advance(Duration d) {
        nanos.addAndGet(d.toNanos());
    }

    @Test
    void burstPassesThenTheNextRequestIsLimitedUntilATokenIsBack() {
        Agent a = agent("agent-a", 1, 3);
        for (int i = 0; i < 3; i++) {
            assertThat(limiter.tryAcquire(a)).as("request " + i).isInstanceOf(Decision.Allowed.class);
        }
        assertThat(limiter.tryAcquire(a)).isEqualTo(new Decision.Limited(1));

        advance(Duration.ofMillis(999));
        assertThat(limiter.tryAcquire(a)).as("a token takes 1 s at 1 rps").isInstanceOf(Decision.Limited.class);
        advance(Duration.ofMillis(1));
        assertThat(limiter.tryAcquire(a)).isInstanceOf(Decision.Allowed.class);
    }

    @Test
    void retryAfterIsRoundedUpToWholeSecondsAndAtLeastOne() {
        Agent fast = agent("agent-fast", 50, 1); // the spec default rate: a token every 20 ms
        limiter.tryAcquire(fast);
        assertThat(limiter.tryAcquire(fast)).isEqualTo(new Decision.Limited(1));
    }

    @Test
    void oneAgentOverItsQuotaDoesNotAffectAnother() {
        Agent noisy = agent("agent-noisy", 1, 2);
        Agent quiet = agent("agent-quiet", 1, 2);
        limiter.tryAcquire(noisy);
        limiter.tryAcquire(noisy);
        assertThat(limiter.tryAcquire(noisy)).isInstanceOf(Decision.Limited.class);
        assertThat(limiter.tryAcquire(quiet)).isInstanceOf(Decision.Allowed.class);
        assertThat(limiter.tryAcquire(quiet)).isInstanceOf(Decision.Allowed.class);
    }

    @Test
    void changedLimitsInTheRegistryTakeEffect() {
        limiter.tryAcquire(agent("agent-a", 1, 1));
        assertThat(limiter.tryAcquire(agent("agent-a", 1, 1))).isInstanceOf(Decision.Limited.class);
        // an operator raised the limits in table agent; the next registry refresh carries the new values
        assertThat(limiter.tryAcquire(agent("agent-a", 50, 100))).isInstanceOf(Decision.Allowed.class);
    }

    @Test
    void theSpecDefaultsAllowABurstOf100() {
        Agent standard = agent("agent-standard", 50, 100);
        int allowed = 0;
        for (int i = 0; i < 150; i++) {
            if (limiter.tryAcquire(standard) instanceof Decision.Allowed) {
                allowed++;
            }
        }
        assertThat(allowed).isEqualTo(100);
        advance(Duration.ofSeconds(1));
        for (int i = 0; i < 50; i++) {
            assertThat(limiter.tryAcquire(standard)).isInstanceOf(Decision.Allowed.class);
        }
        assertThat(limiter.tryAcquire(standard)).as("50 tokens per second").isInstanceOf(Decision.Limited.class);
    }
}
