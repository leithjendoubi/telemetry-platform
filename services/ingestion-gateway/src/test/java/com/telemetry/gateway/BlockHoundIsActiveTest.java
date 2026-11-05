package com.telemetry.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.ongres.scram.common.util.CryptoUtil;
import java.security.SecureRandom;
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

    /**
     * BlockHoundAllowances is loaded and narrow: inside the SCRAM nonce (DB login) blocking is let through, here with
     * a deliberately slow random source; the very same source used anywhere else is still reported.
     */
    @Test
    void theScramNonceAllowanceIsLoadedAndCoversOnlyThatMethod() {
        SecureRandom slow = new SecureRandom() {
            @Override
            public void nextBytes(byte[] bytes) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                super.nextBytes(bytes);
            }
        };
        Mono<String> nonce = Mono.fromCallable(() -> CryptoUtil.nonce(24, slow)).subscribeOn(Schedulers.parallel());
        assertThat(nonce.block(Duration.ofSeconds(5))).hasSize(24);

        Mono<Void> elsewhere = Mono.fromRunnable(() -> slow.nextBytes(new byte[8]))
                .subscribeOn(Schedulers.parallel())
                .then();
        assertThatThrownBy(() -> elsewhere.block(Duration.ofSeconds(5)))
                .hasCauseInstanceOf(BlockingOperationError.class);
    }
}
