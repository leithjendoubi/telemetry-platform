package com.telemetry.analytics.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.telemetry.analytics.ingest.ScoredEventParser.PoisonRecordException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

public class ScoredEventParserTest {

    static final ObjectMapper MAPPER = new ObjectMapper();
    private final ScoredEventParser parser = new ScoredEventParser(MAPPER);

    /** libs/telemetry-contracts/examples/scored-event.json, the same file the detection stub tests against. */
    public static ObjectNode example() {
        try (InputStream in = ScoredEventParserTest.class
                .getClassLoader()
                .getResourceAsStream("contracts/examples/scored-event.json")) {
            return (ObjectNode) MAPPER.readTree(in);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static byte[] bytes(ObjectNode node) {
        return node.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void everyColumnIsMappedFromTheContractExample() {
        TelemetryRow row = parser.parse(bytes(example()));

        assertThat(row.eventTime()).isEqualTo(OffsetDateTime.parse("2026-10-12T09:15:00.000Z"));
        assertThat(row.eventId()).isEqualTo(UUID.fromString("0192a3b4-7c10-7d3e-8a1f-5a2b9c4d6e70"));
        assertThat(row.instanceId()).isEqualTo("checkout-api-07");
        assertThat(row.serviceName()).isEqualTo("checkout-api");
        assertThat(row.environment()).isEqualTo("prod");
        assertThat(row.region()).isEqualTo("eu-south-1");
        assertThat(row.receivedAt()).isEqualTo(OffsetDateTime.parse("2026-10-12T09:15:02.304Z"));
        assertThat(row.scoredAt()).isEqualTo(OffsetDateTime.parse("2026-10-12T09:15:02.391Z"));
        assertThat(row.cpuUsagePct()).isEqualTo(62.4);
        assertThat(row.memUsedBytes()).isEqualTo(1_288_490_188L);
        assertThat(row.memLimitBytes()).isEqualTo(2_147_483_648L);
        assertThat(row.netRxBps()).isEqualTo(1_843_200.0);
        assertThat(row.netTxBps()).isEqualTo(2_211_840.0);
        assertThat(row.diskReadBps()).isEqualTo(20_480.0);
        assertThat(row.diskWriteBps()).isEqualTo(143_360.0);
        assertThat(row.activeThreads()).isEqualTo(118);
        assertThat(row.httpRps()).isEqualTo(342.5);
        assertThat(row.httpErrorPct()).isEqualTo(0.4);
        assertThat(row.httpP95Ms()).isEqualTo(184.0);
        assertThat(row.gcPauseMsMin()).isEqualTo(96.0);
        assertThat(row.anomalyProb()).isEqualTo(0.0);
        assertThat(row.isAnomaly()).isFalse();
        assertThat(row.isWarmup()).isFalse();
        assertThat(row.modelVersion()).isEqualTo("stub-0");
    }

    @Test
    void predictionFieldsComeFromThePredictionObject() {
        ObjectNode e = example();
        ((ObjectNode) e.get("prediction"))
                .put("probability", 0.93)
                .put("isAnomaly", true)
                .put("warmup", true)
                .put("modelVersion", "rf-1.2.0");
        TelemetryRow row = parser.parse(bytes(e));
        assertThat(row.anomalyProb()).isEqualTo(0.93);
        assertThat(row.isAnomaly()).isTrue();
        assertThat(row.isWarmup()).isTrue();
        assertThat(row.modelVersion()).isEqualTo("rf-1.2.0");
    }

    @Test
    void unreadableValuesArePoison() {
        assertThatThrownBy(() -> parser.parse(null))
                .isInstanceOf(PoisonRecordException.class)
                .hasMessageContaining("empty");
        assertThatThrownBy(() -> parser.parse("{oops".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(PoisonRecordException.class)
                .hasMessageContaining("not JSON");
    }

    @Test
    void recordsBreakingTheContractArePoison() {
        assertPoison(e -> e.remove("prediction"));
        assertPoison(e -> e.remove("scoredAt"));
        assertPoison(e -> ((ObjectNode) e.get("prediction")).put("probability", 1.5));
        assertPoison(e -> e.put("eventId", "not-a-uuid"));
        assertPoison(e -> e.put("extra", true));
        // a raw record (no prediction yet) must not be stored by mistake
        assertPoison(e -> {
            e.remove("prediction");
            e.remove("scoredAt");
        });
    }

    @Test
    void valuesTooLargeForTheColumnsArePoison() {
        assertPoison(e -> ((ObjectNode) e.get("metrics")).put("activeThreadCount", 3_000_000_000L), "integer column");
        assertPoison(
                e -> ((ObjectNode) e.get("metrics"))
                        .put("memoryUsedBytes", new java.math.BigInteger("99999999999999999999")),
                "bigint column");
    }

    private void assertPoison(Consumer<ObjectNode> change) {
        assertPoison(change, "");
    }

    private void assertPoison(Consumer<ObjectNode> change, String message) {
        ObjectNode e = example();
        change.accept(e);
        assertThatThrownBy(() -> parser.parse(bytes(e)))
                .isInstanceOf(PoisonRecordException.class)
                .hasMessageContaining(message);
    }
}
