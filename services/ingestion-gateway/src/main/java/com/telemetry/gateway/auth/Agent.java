package com.telemetry.gateway.auth;

import java.util.Set;

/** An authenticated agent: who sent the request and which services it may report on. */
public record Agent(String agentId, Set<String> allowedServices) {

    public Agent {
        allowedServices = Set.copyOf(allowedServices);
    }
}
