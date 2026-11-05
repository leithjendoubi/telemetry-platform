package com.telemetry.gateway.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import javax.management.ObjectName;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import reactor.kafka.sender.KafkaSender;

/**
 * FR-ING-02: the Kafka producer must exist when the gateway starts, not be created by the first send on a Netty
 * event-loop thread (creating it reads files and starts threads; found by BlockHound on 2026-11-05). BlockHound alone
 * cannot always see this: once another Kafka client in the same JVM has loaded the classes, a late creation leaves no
 * blocking call behind. So this test looks for the producer's JMX registration ("app-info", made by the producer's
 * constructor) right after the bean is built, before anything is sent. A unique client id makes it independent of
 * every other test. No broker is needed: creating a producer does not connect.
 */
class KafkaSenderStartupTest {

    /** Spec 6.8: producer timeout 2 s; Kafka itself checks that the timeouts fit together (ProducerConfig). */
    @Test
    void producerTimeoutIsTheTwoSecondsOfSpec68AndKafkaAcceptsTheSettings() {
        ProducerConfig config = new ProducerConfig(GatewayConfig.producerConfig(props(), "timeout-test"));
        assertThat(config.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)).isEqualTo(2_000);
        assertThat(config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)
                        + config.getLong(ProducerConfig.LINGER_MS_CONFIG))
                .as("Kafka requires delivery.timeout.ms >= linger.ms + request.timeout.ms")
                .isLessThanOrEqualTo(2_000);
        assertThat(config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG)).isLessThanOrEqualTo(2_000L);
        assertThat(config.getString(ProducerConfig.ACKS_CONFIG)).isEqualTo("-1"); // "all"
        assertThat(config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isTrue();
    }

    @Test
    void theProducerExistsBeforeTheFirstSend() throws Exception {
        String clientId = "startup-test-" + UUID.randomUUID();
        GatewayProperties props = props();
        ObjectName appInfo = new ObjectName("kafka.producer:type=app-info,id=" + clientId);

        KafkaSender<String, byte[]> sender = new GatewayConfig().kafkaSender(props, clientId);
        try {
            assertThat(ManagementFactory.getPlatformMBeanServer().isRegistered(appInfo))
                    .as("producer created at start-up, before any send")
                    .isTrue();
        } finally {
            sender.close();
        }
    }

    private static GatewayProperties props() {
        return new GatewayProperties(
                262_144,
                100,
                Duration.ofHours(24),
                Duration.ofMinutes(5),
                Duration.ofSeconds(5),
                2000,
                new GatewayProperties.Kafka("localhost:1", "telemetry.raw.v1", "telemetry.rejected.v1", Map.of()),
                new GatewayProperties.Registry(Duration.ofSeconds(10), 10_000, Duration.ofSeconds(2)));
    }
}
