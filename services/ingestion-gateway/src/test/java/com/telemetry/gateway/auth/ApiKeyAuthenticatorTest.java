package com.telemetry.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import com.telemetry.gateway.config.GatewayProperties.AgentEntry;
import org.junit.jupiter.api.Test;

class ApiKeyAuthenticatorTest {

    private final ApiKeyAuthenticator authenticator = new ApiKeyAuthenticator(List.of(
            new AgentEntry("agent-checkout-api", ApiKeyAuthenticator.sha256Hex("dev-agent-checkout-api"), List.of("checkout-api")),
            new AgentEntry("agent-cart-api", ApiKeyAuthenticator.sha256Hex("dev-agent-cart-api").toUpperCase(), List.of("cart-api"))));

    @Test
    void knownKeyGivesItsAgent() {
        assertThat(authenticator.authenticate("dev-agent-checkout-api")).hasValueSatisfying(agent -> {
            assertThat(agent.agentId()).isEqualTo("agent-checkout-api");
            assertThat(agent.allowedServices()).containsExactly("checkout-api");
        });
        assertThat(authenticator.authenticate("dev-agent-cart-api")).get().extracting(Agent::agentId).isEqualTo("agent-cart-api");
    }

    @Test
    void missingOrUnknownKeyGivesNothing() {
        assertThat(authenticator.authenticate(null)).isEmpty();
        assertThat(authenticator.authenticate("")).isEmpty();
        assertThat(authenticator.authenticate("dev-agent-checkout-ap")).isEmpty();
        // the hash itself is not a key
        assertThat(authenticator.authenticate(ApiKeyAuthenticator.sha256Hex("dev-agent-checkout-api"))).isEmpty();
    }

    @Test
    void sha256MatchesAKnownValue() {
        // printf 'abc' | sha256sum
        assertThat(ApiKeyAuthenticator.sha256Hex("abc"))
                .isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }

    @Test
    void badConfigurationFailsAtStartup() {
        assertThatThrownBy(() -> new ApiKeyAuthenticator(List.of(new AgentEntry("a", "dev-key-in-clear", List.of("s")))))
                .hasMessageContaining("64 hex");
        String hash = ApiKeyAuthenticator.sha256Hex("k");
        assertThatThrownBy(() -> new ApiKeyAuthenticator(List.of(
                new AgentEntry("a", hash, List.of("s")), new AgentEntry("b", hash, List.of("s")))))
                .hasMessageContaining("share one API key");
    }
}
