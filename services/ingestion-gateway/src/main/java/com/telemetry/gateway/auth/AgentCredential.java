package com.telemetry.gateway.auth;

import java.util.Set;

/**
 * One registered agent as the gateway needs it (a row of the agent table, spec 8.4): who it is, the SHA-256 of its API
 * key, the services it may report on, its status and its rate limit (FR-ING-05, FR-ING-06). Kept in this package so
 * authentication does not depend on where agents are stored.
 */
public record AgentCredential(
        String agentId,
        String apiKeySha256,
        Set<String> allowedServices,
        AgentStatus status,
        int rateLimitRps,
        int burst) {

    public AgentCredential {
        allowedServices = Set.copyOf(allowedServices);
    }

    /** Values of the agent.status column. */
    public enum AgentStatus {
        ACTIVE,
        REVOKED
    }
}
