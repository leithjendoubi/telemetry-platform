package com.telemetry.gateway.auth;

import java.util.Set;

/**
 * An authenticated agent: who sent the request, which services it may report on, and its rate limit (FR-ING-06:
 * {@code rateLimitRps} requests per second on average, at most {@code burst} at once).
 */
public record Agent(String agentId, Set<String> allowedServices, int rateLimitRps, int burst) {

    public Agent {
        allowedServices = Set.copyOf(allowedServices);
        if (rateLimitRps <= 0 || burst <= 0) {
            throw new IllegalArgumentException("agent " + agentId + ": rate limit and burst must be positive");
        }
    }
}
