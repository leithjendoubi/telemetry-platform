package com.telemetry.gateway;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * FR-ING-02: BlockHound (installed for every test by blockhound-junit-platform, pom.xml) fails a test when a
 * non-blocking thread blocks. This test proves it is really on: if the JVM flag or the dependency went missing,
 * every other test would pass without checking anything, and this one would fail.
 */
class BlockHoundIsActiveTest {

    @Test
    void sleepingOnANonBlockingThreadIsReported() {
        Mono<Void> blocking = Mono.fromRunnable(() -> {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                })
                .subscribeOn(Schedulers.parallel())
                .then();

        assertThatThrownBy(() -> blocking.block(Duration.ofSeconds(5)))
                .hasCauseInstanceOf(BlockingOperationError.class)
                .hasMessageContaining("Thread.sleep");
    }
}
