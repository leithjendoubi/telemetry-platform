package com.telemetry.gateway.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class InFlightLimiterTest {

    @Test
    void slotsAreTakenAndFreed() {
        InFlightLimiter limiter = new InFlightLimiter(2);
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).as("both slots taken").isFalse();
        limiter.release();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.inFlight()).isEqualTo(2);
    }

    @Test
    void neverMoreThanMaxUnderConcurrency() throws Exception {
        InFlightLimiter limiter = new InFlightLimiter(10);
        AtomicInteger granted = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(64);
        for (int i = 0; i < 64; i++) {
            pool.submit(() -> {
                start.await();
                if (limiter.tryAcquire()) {
                    granted.incrementAndGet();
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        assertThat(granted).hasValue(10);
        assertThat(limiter.inFlight()).isEqualTo(10);
    }

    @Test
    void aZeroLimitIsAConfigurationError() {
        assertThatThrownBy(() -> new InFlightLimiter(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
