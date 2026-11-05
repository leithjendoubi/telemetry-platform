package com.telemetry.gateway.validation;

import static org.assertj.core.api.Assertions.assertThat;

import com.telemetry.gateway.validation.ValidationOutcome.Rejection;
import org.junit.jupiter.api.Test;

/** The rejection message is capped at the 1000 characters of telemetry-rejected-event.1.0.json. */
class RejectionTest {

    private static Rejection withMessage(String message) {
        return new Rejection(0, "id", RejectionCode.SCHEMA_VIOLATION, message);
    }

    @Test
    void shortMessageIsKept() {
        String exactlyMax = "m".repeat(Rejection.MAX_MESSAGE_LENGTH);
        assertThat(withMessage("short").message()).isEqualTo("short");
        assertThat(withMessage(exactlyMax).message()).isEqualTo(exactlyMax);
    }

    @Test
    void longMessageQuotingAgentInputIsCutAndMarked() {
        String message = "property '" + "x".repeat(200_000) + "' is not defined in the schema";
        String cut = withMessage(message).message();
        assertThat(cut)
                .hasSize(Rejection.MAX_MESSAGE_LENGTH)
                .startsWith("property 'xxx")
                .endsWith(" [...]");
    }

    @Test
    void cutNeverSplitsATwoCharCharacter() {
        String emoji = "😀"; // one character, two Java chars
        int cutAt = Rejection.MAX_MESSAGE_LENGTH - " [...]".length();
        String message = "a".repeat(cutAt - 1) + emoji + "tail".repeat(100);
        String cut = withMessage(message).message();
        assertThat(cut).hasSizeLessThanOrEqualTo(Rejection.MAX_MESSAGE_LENGTH).endsWith("a [...]");
        assertThat(cut.chars().filter(c -> Character.isSurrogate((char) c))).isEmpty();
    }
}
