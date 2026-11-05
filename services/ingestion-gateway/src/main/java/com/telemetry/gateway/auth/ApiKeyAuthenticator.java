package com.telemetry.gateway.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Checks the X-API-Key header (FR-ING-05, skeleton version).
 *
 * <p>Only SHA-256 hashes of the keys are kept. The presented key is hashed and the hash is looked up,
 * so the real keys never sit in memory or in configuration. Looking up a hash does not leak the key
 * through timing: an attacker would first need a key with a chosen hash, which SHA-256 makes infeasible.
 */
public class ApiKeyAuthenticator {

    private static final Pattern SHA256_HEX = Pattern.compile("^[0-9a-f]{64}$");

    private final Map<String, Agent> agentsByKeyHash = new HashMap<>();

    public ApiKeyAuthenticator(List<AgentCredential> entries) {
        for (AgentCredential entry : entries) {
            String hash =
                    entry.apiKeySha256() == null ? "" : entry.apiKeySha256().toLowerCase(Locale.ROOT);
            if (!SHA256_HEX.matcher(hash).matches()) {
                throw new IllegalArgumentException(
                        "agent " + entry.agentId() + ": api-key-sha256 must be 64 hex characters");
            }
            if (entry.agentId() == null
                    || entry.allowedServices() == null
                    || entry.allowedServices().isEmpty()) {
                throw new IllegalArgumentException("every agent needs an agent-id and at least one allowed service");
            }
            Agent previous = agentsByKeyHash.put(hash, new Agent(entry.agentId(), Set.copyOf(entry.allowedServices())));
            if (previous != null) {
                throw new IllegalArgumentException(
                        "agents " + previous.agentId() + " and " + entry.agentId() + " share one API key");
            }
        }
    }

    /** The agent owning this key, or empty when the key is missing or unknown. */
    public Optional<Agent> authenticate(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(agentsByKeyHash.get(sha256Hex(apiKey)));
    }

    public int agentCount() {
        return agentsByKeyHash.size();
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
