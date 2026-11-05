package com.telemetry.gateway.validation;

import static org.assertj.core.api.Assertions.assertThat;

import com.networknt.schema.regex.RegularExpression;
import org.junit.jupiter.api.Test;

/** Schema patterns read with the ECMA-262 '$' (contract README, rule 4); the Python contract tests do the same. */
class EcmaRegularExpressionFactoryTest {

    private static final String IDENTIFIER = "^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"; // from telemetry-batch.1.0.json

    @Test
    void dollarBecomesTheTrueEndOfText() {
        assertThat(EcmaRegularExpressionFactory.ecmaAnchors("^[a-z]{0,3}$")).isEqualTo("^[a-z]{0,3}\\z");
        assertThat(EcmaRegularExpressionFactory.ecmaAnchors("^a\\$b$"))
                .as("an escaped $ is a literal dollar")
                .isEqualTo("^a\\$b\\z");
        assertThat(EcmaRegularExpressionFactory.ecmaAnchors("^[$]+$"))
                .as("inside [...] too")
                .isEqualTo("^[$]+\\z");
    }

    @Test
    void identifierPatternRefusesATrailingNewlineThatJavaAloneWouldAccept() {
        RegularExpression identifier = EcmaRegularExpressionFactory.INSTANCE.getRegularExpression(IDENTIFIER);
        assertThat(identifier.matches("checkout-api-07")).isTrue();
        assertThat(identifier.matches("checkout-api-07\n")).isFalse();
        assertThat(identifier.matches("checkout-api-07\r\n")).isFalse();
        // the reason for this class: plain java.util.regex lets the trailing newline through
        assertThat(java.util.regex.Pattern.compile(IDENTIFIER)
                        .matcher("checkout-api-07\n")
                        .find())
                .isTrue();
    }
}
