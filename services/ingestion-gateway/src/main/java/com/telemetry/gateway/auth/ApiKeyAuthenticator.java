package com.telemetry.gateway.auth;

import com.telemetry.gateway.auth.AgentCredential.AgentStatus;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;

/**
 * The rules of the X-API-Key check (FR-ING-05, AT-03); where agents are stored is not its business.
 *
 * <p>Only SHA-256 hashes of keys are stored. The presented key is hashed and the agent is looked up by that hash, so
 * the real keys never sit in memory, configuration or the database. Looking up a hash does not leak the key through
 * timing: an attacker would first need a key with a chosen hash, which SHA-256 makes infeasible.
 */
public final class ApiKeyAuthenticator {

    /** Outcome of the check. Answers follow the error table of spec 8.1. */
    public sealed interface AuthResult {

        /** No key, or a key no agent has: 401. */
        record Unknown() implements AuthResult {}

        /** The key belongs to a revoked agent: 403. */
        record Revoked(String agentId) implements AuthResult {}

        /** An active agent: the request goes on. */
        record Authenticated(Agent agent) implements AuthResult {}
    }

    private ApiKeyAuthenticator() {}

    /** The SHA-256 to look up, or empty when the header is missing or blank (401 without asking the registry). */
    public static Optional<String> keyHash(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(sha256Hex(apiKey));
    }

    /** Decides on the agent the registry found for the key's hash (empty: no such agent). */
    public static AuthResult decide(Optional<AgentCredential> found) {
        if (found.isEmpty()) {
            return new AuthResult.Unknown();
        }
        AgentCredential agent = found.get();
        if (agent.status() != AgentStatus.ACTIVE) {
            return new AuthResult.Revoked(agent.agentId());
        }
        return new AuthResult.Authenticated(
                new Agent(agent.agentId(), agent.allowedServices(), agent.rateLimitRps(), agent.burst()));
    }

    public static String sha256Hex(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("every Java runtime has SHA-256", e);
        }
    }
}
