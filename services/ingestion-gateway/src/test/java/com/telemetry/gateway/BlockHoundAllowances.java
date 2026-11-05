package com.telemetry.gateway;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

/**
 * The only calls BlockHound may let through on non-blocking threads (ADR-0011). Loaded by BlockHound through
 * META-INF/services. Each entry names one method and says why its "blocking" call is not a real block.
 */
public class BlockHoundAllowances implements BlockHoundIntegration {

    @Override
    public void applyTo(BlockHound.Builder builder) {
        // When R2DBC opens a new connection, PostgreSQL's SCRAM-SHA-256 login needs a random nonce. On Linux, Java's
        // default SecureRandom (NativePRNG) reads it from /dev/urandom: a file read for BlockHound, but it never waits
        // and happens only when the pool opens a connection. Found by CI on Linux on 2026-11-05 (Windows uses an OS
        // call instead). Allowed only inside this one method.
        builder.allowBlockingCallsInside("com.ongres.scram.common.util.CryptoUtil", "nonce");
    }
}
