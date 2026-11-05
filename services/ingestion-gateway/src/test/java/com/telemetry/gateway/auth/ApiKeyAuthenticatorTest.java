package com.telemetry.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.telemetry.gateway.auth.AgentCredential.AgentStatus;
import com.telemetry.gateway.auth.ApiKeyAuthenticator.AuthResult;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ApiKeyAuthenticatorTest {

    private static AgentCredential agent(AgentStatus status) {
        return new AgentCredential(
                "agent-checkout-api",
                ApiKeyAuthenticator.sha256Hex("dev-agent-checkout-api"),
                Set.of("checkout-api"),
                status,
                50,
                100);
    }

    @Test
    void activeAgentIsAuthenticatedWithItsServices() {
        AuthResult result = ApiKeyAuthenticator.decide(Optional.of(agent(AgentStatus.ACTIVE)));
        assertThat(result).isInstanceOfSatisfying(AuthResult.Authenticated.class, ok -> {
            assertThat(ok.agent().agentId()).isEqualTo("agent-checkout-api");
            assertThat(ok.agent().allowedServices()).containsExactly("checkout-api");
        });
    }

    @Test
    void revokedAgentIsRevokedNotUnknown() {
        assertThat(ApiKeyAuthenticator.decide(Optional.of(agent(AgentStatus.REVOKED))))
                .isEqualTo(new AuthResult.Revoked("agent-checkout-api"));
    }

    @Test
    void noAgentIsUnknown() {
        assertThat(ApiKeyAuthenticator.decide(Optional.empty())).isInstanceOf(AuthResult.Unknown.class);
    }

    @Test
    void missingOrBlankKeyIsNotLookedUp() {
        assertThat(ApiKeyAuthenticator.keyHash(null)).isEmpty();
        assertThat(ApiKeyAuthenticator.keyHash("")).isEmpty();
        assertThat(ApiKeyAuthenticator.keyHash("   ")).isEmpty();
        assertThat(ApiKeyAuthenticator.keyHash("dev-agent-checkout-api"))
                .contains(ApiKeyAuthenticator.sha256Hex("dev-agent-checkout-api"));
    }

    @Test
    void sha256MatchesAKnownValue() {
        // printf 'abc' | sha256sum
        assertThat(ApiKeyAuthenticator.sha256Hex("abc"))
                .isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }
}
