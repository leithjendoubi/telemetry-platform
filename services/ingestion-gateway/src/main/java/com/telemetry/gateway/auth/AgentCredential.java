package com.telemetry.gateway.auth;

import java.util.List;

/**
 * One registered agent as the authenticator needs it: who it is, the SHA-256 of its API key, and the services it
 * may report on. Kept in this package so authentication does not depend on how the settings are loaded.
 */
public record AgentCredential(String agentId, String apiKeySha256, List<String> allowedServices) {}
