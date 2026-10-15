package com.telemetry.analytics;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.telemetry.analytics.ingest.ScoredEventParserTest;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * The whole service against a real Kafka and a real TimescaleDB, both the same images as infra/compose.
 * The database is prepared by the real init script (infra/compose/init-db/01-create-databases.sh), so the
 * service connects exactly like in compose: database analytics_db, role analytics, extension pre-installed.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class AnalyticsIntegrationTest {

    static final String TOPIC = "telemetry.scored.v1";
    static final String DLQ = "telemetry.scored.v1.dlq";
    static final String GROUP = "analytics-writer";
    static final String SUPER_PASSWORD = "test_super";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.1");

    @Container
    static final PostgreSQLContainer<?> DB = new PostgreSQLContainer<>(
            DockerImageName.parse("timescale/timescaledb:2.17.2-pg16").asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("postgres").withUsername("postgres").withPassword(SUPER_PASSWORD)
            .withEnv(Map.of(
                    "INGESTION_DB_USER", "ingestion", "INGESTION_DB_PASSWORD", "ingestion_test",
                    "ANALYTICS_DB_USER", "analytics", "ANALYTICS_DB_PASSWORD", "analytics_test",
                    "ALERTING_DB_USER", "alerting", "ALERTING_DB_PASSWORD", "alerting_test"))
            .withCopyFileToContainer(MountableFile.forClasspathResource("init-db/01-create-databases.sh", 0755),
                    "/docker-entrypoint-initdb.d/01-create-databases.sh");

    static KafkaProducer<String, byte[]> producer;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", () -> analyticsDbUrl());
        registry.add("spring.datasource.username", () -> "analytics");
        registry.add("spring.datasource.password", () -> "analytics_test");
        registry.add("analytics.kafka.retry-interval-ms", () -> "500");
    }

    @BeforeAll
    static void createTopics() throws Exception {
        try (AdminClient admin = admin()) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 12, (short) 1), new NewTopic(DLQ, 3, (short) 1))).all().get();
        }
        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    }

    @AfterAll
    static void closeProducer() {
        producer.close();
    }

    @Autowired
    TestRestTemplate http;

    // ------------------------------------------------------------------ schema

    @Test
    void flywayCreatedTheTableOfSpec84() throws Exception {
        try (Connection c = superuser(); Statement st = c.createStatement()) {
            assertThat(single(st, "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'telemetry_event'"))
                    .isEqualTo("1");
            assertThat(single(st, "SELECT tableowner FROM pg_tables WHERE tablename = 'telemetry_event'")).isEqualTo("analytics");
            assertThat(single(st, """
                    SELECT string_agg(a.attname, ',' ORDER BY k.ord)
                    FROM pg_index i
                    CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                    WHERE i.indrelid = 'telemetry_event'::regclass AND i.indisprimary""")).isEqualTo("instance_id,event_time,event_id");
            assertThat(single(st, "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'telemetry_event'")).isEqualTo("24");
            assertThat(single(st, "SELECT string_agg(indexname, ',' ORDER BY indexname) FROM pg_indexes "
                    + "WHERE tablename = 'telemetry_event' AND indexname LIKE 'ix_%'")).isEqualTo("ix_event_anomaly_time,ix_event_service_time");
            assertThat(single(st, "SELECT column_name || ' ' || time_interval FROM timescaledb_information.dimensions "
                    + "WHERE hypertable_name = 'telemetry_event'")).isEqualTo("event_time 1 day");
        }
    }

    // ------------------------------------------------------------------ writing

    @Test
    void scoredEventsAreStoredOnceAndPoisonGoesToTheDlq() throws Exception {
        String instance = "writer-" + UUID.randomUUID().toString().substring(0, 8);
        List<ObjectNode> events = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            events.add(scored(instance, Instant.now().minusSeconds(600 - i * 10L)));
        }
        events.forEach(this::send);
        events.subList(0, 10).forEach(this::send); // replayed copies, as after a crash
        producer.send(new ProducerRecord<>(TOPIC, instance, "{broken".getBytes())).get();
        producer.flush();

        assertThat(waitFor(() -> rows(instance) == 50, 30)).as("50 rows, the 10 copies ignored").isTrue();
        assertThat(waitFor(this::allCommitted, 30)).as("offsets committed after the database commit").isTrue();
        Thread.sleep(1000);
        assertThat(rows(instance)).isEqualTo(50);

        // one row, column by column, against what was sent
        ObjectNode sent = events.get(7);
        try (Connection c = superuser(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM telemetry_event WHERE event_id = '" + sent.get("eventId").asText() + "'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getTimestamp("event_time").toInstant()).isEqualTo(Instant.parse(sent.get("timestamp").asText()));
            assertThat(rs.getString("instance_id")).isEqualTo(instance);
            assertThat(rs.getString("service_name")).isEqualTo("checkout-api");
            assertThat(rs.getTimestamp("received_at").toInstant()).isEqualTo(Instant.parse(sent.get("receivedAt").asText()));
            assertThat(rs.getTimestamp("scored_at").toInstant()).isEqualTo(Instant.parse(sent.get("scoredAt").asText()));
            assertThat(rs.getFloat("cpu_usage_pct")).isEqualTo(62.4f);
            assertThat(rs.getLong("mem_limit_bytes")).isEqualTo(2_147_483_648L);
            assertThat(rs.getInt("active_threads")).isEqualTo(118);
            assertThat(rs.getFloat("anomaly_prob")).isEqualTo(0.0f);
            assertThat(rs.getBoolean("is_anomaly")).isFalse();
            assertThat(rs.getString("model_version")).isEqualTo("stub-0");
        }

        List<ConsumerRecord<byte[], byte[]>> dead = readTopic(DLQ, 1);
        assertThat(dead).hasSize(1);
        assertThat(new String(dead.get(0).value())).isEqualTo("{broken");
        assertThat(new String(dead.get(0).headers().lastHeader("dlq-error").value())).contains("not JSON");
        assertThat(new String(dead.get(0).headers().lastHeader("dlq-source-topic").value())).isEqualTo(TOPIC);
    }

    @Test
    void databaseOutageStopsCommitsAndNothingIsLost() throws Exception {
        assertThat(waitFor(this::allCommitted, 30)).isTrue();
        String instance = "outage-" + UUID.randomUUID().toString().substring(0, 8);
        try (Connection c = superuser(); Statement st = c.createStatement()) {
            st.execute("ALTER TABLE telemetry_event RENAME TO telemetry_event_offline"); // inserts now fail
            try {
                for (int i = 0; i < 5; i++) {
                    send(scored(instance, Instant.now().minusSeconds(60 - i)));
                }
                producer.flush();
                Thread.sleep(4000); // several failed retries
                assertThat(uncommitted()).as("nothing committed while the database fails").isEqualTo(5);
            } finally {
                st.execute("ALTER TABLE telemetry_event_offline RENAME TO telemetry_event");
            }
        }
        assertThat(waitFor(() -> rows(instance) == 5, 30)).as("the retried batch arrives once the table is back").isTrue();
        assertThat(waitFor(this::allCommitted, 30)).isTrue();
    }

    // ------------------------------------------------------------------ query endpoint

    @Test
    void seriesEndpointReturnsThePointsInOrder() throws Exception {
        String instance = "query-" + UUID.randomUUID().toString().substring(0, 8);
        Instant base = Instant.now().truncatedTo(ChronoUnit.SECONDS).minusSeconds(3600);
        for (int i = 4; i >= 0; i--) { // sent in reverse order on purpose
            ObjectNode e = scored(instance, base.plusSeconds(15L * i));
            ((ObjectNode) e.get("metrics")).put("cpuUsagePercent", 10.0 + i);
            send(e);
        }
        producer.flush();
        assertThat(waitFor(() -> rows(instance) == 5, 30)).isTrue();

        ResponseEntity<JsonNode> response = http.getForEntity(
                "/api/v1/analytics/instances/{id}/series?metric=cpuUsagePercent&from={from}&to={to}", JsonNode.class,
                instance, base.toString(), base.plusSeconds(60).toString());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode body = response.getBody();
        assertThat(body.get("instanceId").asText()).isEqualTo(instance);
        assertThat(body.get("truncated").asBoolean()).isFalse();
        JsonNode points = body.get("points");
        assertThat(points).hasSize(4); // 'to' is exclusive: the point at base+60s is outside
        for (int i = 0; i < 4; i++) {
            assertThat(Instant.parse(points.get(i).get("time").asText())).isEqualTo(base.plusSeconds(15L * i));
            assertThat(points.get(i).get("value").asDouble()).isEqualTo(10.0 + i);
            assertThat(points.get(i).get("anomalyProb").asDouble()).isZero();
            assertThat(points.get(i).get("isAnomaly").asBoolean()).isFalse();
        }
    }

    @Test
    void seriesEndpointStopsAt2000Points() throws Exception {
        String instance = "bulk-" + UUID.randomUUID().toString().substring(0, 8);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        try (Connection c = superuser(); Statement st = c.createStatement()) {
            st.execute("""
                    INSERT INTO telemetry_event SELECT
                        timestamptz '2026-01-01T00:00:00Z' + n * interval '15 seconds', gen_random_uuid(), '%s', 'bulk-api', 'prod', 'eu-west-1',
                        now(), now(), n %% 100, 1, 2, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, false, false, 'stub-0'
                    FROM generate_series(0, 2099) AS n""".formatted(instance));
        }
        JsonNode body = http.getForObject("/api/v1/analytics/instances/{id}/series?metric=cpuUsagePercent&from={from}&to={to}",
                JsonNode.class, instance, base.toString(), base.plus(Duration.ofDays(1)).toString());
        assertThat(body.get("points")).hasSize(2000);
        assertThat(body.get("truncated").asBoolean()).isTrue();
    }

    @Test
    void badQueriesAre400ProblemDetails() {
        String url = "/api/v1/analytics/instances/{id}/series?metric={m}&from={f}&to={t}";
        String f = "2026-10-14T00:00:00Z";
        String t = "2026-10-14T01:00:00Z";
        assertBadRequest(http.getForEntity(url, JsonNode.class, "x-01", "cpu; DROP TABLE telemetry_event", f, t), "unknown metric");
        assertBadRequest(http.getForEntity(url, JsonNode.class, "bad id!", "cpuUsagePercent", f, t), "instanceId");
        assertBadRequest(http.getForEntity(url, JsonNode.class, "x-01", "cpuUsagePercent", t, f), "after");
        assertBadRequest(http.getForEntity(url, JsonNode.class, "x-01", "cpuUsagePercent", "yesterday", t), "RFC 3339");
        assertBadRequest(http.getForEntity(url, JsonNode.class, "x-01", "cpuUsagePercent", "2026-01-01T00:00:00Z", t), "at most");
        assertBadRequest(http.getForEntity("/api/v1/analytics/instances/x-01/series?metric=cpuUsagePercent", JsonNode.class), "from");
    }

    @Test
    void healthProbesIncludeTheDatabase() {
        assertThat(http.getForObject("/health/live", JsonNode.class).get("status").asText()).isEqualTo("UP");
        JsonNode ready = http.getForObject("/health/ready", JsonNode.class);
        assertThat(ready.get("status").asText()).isEqualTo("UP");
    }

    // ------------------------------------------------------------------ helpers

    private static void assertBadRequest(ResponseEntity<JsonNode> response, String detailPart) {
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getHeaders().getContentType().toString()).startsWith("application/problem+json");
        assertThat(response.getBody().get("detail").asText()).contains(detailPart);
    }

    /** The contract example with a fresh eventId, the given instance and timestamp. */
    static ObjectNode scored(String instance, Instant timestamp) {
        ObjectNode e = ScoredEventParserTest.example();
        e.put("eventId", UUID.randomUUID().toString());
        e.put("timestamp", timestamp.truncatedTo(ChronoUnit.MILLIS).toString());
        e.put("receivedAt", timestamp.plusSeconds(2).truncatedTo(ChronoUnit.MILLIS).toString());
        e.put("scoredAt", timestamp.plusSeconds(3).truncatedTo(ChronoUnit.MILLIS).toString());
        ((ObjectNode) e.get("resource")).put("instanceId", instance);
        return e;
    }

    private void send(ObjectNode event) {
        producer.send(new ProducerRecord<>(TOPIC, event.get("resource").get("instanceId").asText(), ScoredEventParserTest.bytes(event)));
    }

    private static String analyticsDbUrl() {
        return "jdbc:postgresql://" + DB.getHost() + ":" + DB.getMappedPort(5432) + "/analytics_db";
    }

    private static Connection superuser() throws SQLException {
        return DriverManager.getConnection(analyticsDbUrl(), "postgres", SUPER_PASSWORD);
    }

    private static String single(Statement st, String sql) throws SQLException {
        try (ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private long rows(String instance) {
        try (Connection c = superuser(); Statement st = c.createStatement()) {
            return Long.parseLong(single(st, "SELECT count(*) FROM telemetry_event WHERE instance_id = '" + instance + "'"));
        } catch (SQLException e) {
            return -1; // e.g. while the table is renamed
        }
    }

    private static AdminClient admin() {
        return AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    }

    /** Records in the topic that the analytics-writer group has not committed yet. */
    private long uncommitted() {
        try (AdminClient admin = admin()) {
            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            for (int p = 0; p < 12; p++) {
                request.put(new TopicPartition(TOPIC, p), OffsetSpec.latest());
            }
            long end = admin.listOffsets(request).all().get().values().stream().mapToLong(o -> o.offset()).sum();
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get();
            long done = committed.values().stream().filter(o -> o != null).mapToLong(OffsetAndMetadata::offset).sum();
            return end - done;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private boolean allCommitted() {
        return uncommitted() == 0;
    }

    private static List<ConsumerRecord<byte[], byte[]>> readTopic(String topic, int expected) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "reader-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (out.size() < expected && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(300)).forEach(out::add);
            }
        }
        return out;
    }

    private static boolean waitFor(BooleanSupplier condition, int seconds) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(seconds).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(250);
        }
        return false;
    }
}
