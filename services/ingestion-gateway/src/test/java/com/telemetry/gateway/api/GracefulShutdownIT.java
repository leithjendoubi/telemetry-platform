package com.telemetry.gateway.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.telemetry.gateway.IngestionGatewayApplication;
import com.telemetry.gateway.TestDatabase;
import com.telemetry.gateway.ratelimit.InFlightLimiter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * FR-ING-11: on shutdown (SIGTERM in a container) the gateway stops accepting requests, completes the ones in progress
 * and flushes the producer, within 30 s (server.shutdown=graceful, spring.lifecycle.timeout-per-shutdown-phase).
 * The test starts its own gateway, so closing it does not disturb the other test classes: a request is held in
 * progress (headers sent, body held back), the gateway is closed, new connections must be refused, and the held
 * request must still get 202 and its event must be in Kafka.
 */
@Testcontainers
class GracefulShutdownIT {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.1");

    @Test
    void inFlightRequestCompletesWhileNewConnectionsAreRefused() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                            new NewTopic("telemetry.raw.v1", 12, (short) 1),
                            new NewTopic("telemetry.rejected.v1", 3, (short) 1)))
                    .all()
                    .get();
        }
        List<String> args = new ArrayList<>(TestDatabase.asArguments());
        args.add("--server.port=0");
        args.add("--gateway.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers());
        ConfigurableApplicationContext gateway =
                new SpringApplicationBuilder(IngestionGatewayApplication.class).run(args.toArray(String[]::new));
        int port = Integer.parseInt(gateway.getEnvironment().getProperty("local.server.port"));
        InFlightLimiter inFlight = gateway.getBean(InFlightLimiter.class);

        var batch = IngestControllerIT.freshBatch(1);
        byte[] body = batch.toString().getBytes(StandardCharsets.UTF_8);
        Thread closer = new Thread(gateway::close, "test-shutdown");
        try (Socket held = new Socket("localhost", port)) {
            held.setSoTimeout(40_000);
            OutputStream out = held.getOutputStream();
            out.write(("POST " + IngestControllerIT.URL + " HTTP/1.1\r\nHost: localhost\r\n"
                            + "Content-Type: application/json\r\nX-API-Key: " + IngestControllerIT.KEY + "\r\n"
                            + "Content-Length: " + body.length + "\r\n\r\n")
                    .getBytes(StandardCharsets.UTF_8));
            out.flush();
            waitUntil(() -> inFlight.inFlight() == 1, Duration.ofSeconds(15), "the held request is in progress");

            closer.start(); // what SIGTERM does in a container
            waitUntil(() -> refusesConnections(port), Duration.ofSeconds(10), "new connections are refused");

            // a slow client: even server.shutdown=immediate lets connections finish for a few seconds, so only a
            // request that needs longer tells the 30 s graceful shutdown apart (a planted "immediate" passed without
            // this)
            Thread.sleep(6_000);
            out.write(body); // the held request goes on during the shutdown
            out.flush();
            String statusLine = new BufferedReader(
                            new InputStreamReader(held.getInputStream(), StandardCharsets.US_ASCII))
                    .readLine();
            assertThat(statusLine).as("the request in progress is completed").startsWith("HTTP/1.1 202");
        }
        closer.join(Duration.ofSeconds(35).toMillis());
        assertThat(closer.isAlive())
                .as("shutdown finished within the 30 s budget")
                .isFalse();
        assertThat(gateway.isActive()).isFalse();
        assertThat(eventIdsOnRawTopic())
                .as("its event was stored in Kafka")
                .contains(batch.get("events").get(0).get("eventId").asText());
    }

    private static boolean refusesConnections(int port) {
        try (Socket probe = new Socket("localhost", port)) {
            return false;
        } catch (IOException e) {
            return true;
        }
    }

    private static List<String> eventIdsOnRawTopic() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "graceful-shutdown-test");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        List<String> ids = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of("telemetry.raw.v1"));
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (ids.isEmpty() && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(300))) {
                    ids.add(IngestControllerIT.readTree(r.value())
                            .get("eventId")
                            .asText());
                }
            }
        }
        return ids;
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition, Duration patience, String what)
            throws InterruptedException {
        long deadline = System.nanoTime() + patience.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertThat(condition.getAsBoolean()).as(what).isTrue();
    }
}
