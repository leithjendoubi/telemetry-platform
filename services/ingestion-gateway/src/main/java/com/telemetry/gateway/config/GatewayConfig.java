package com.telemetry.gateway.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.gateway.auth.AgentCredential;
import com.telemetry.gateway.auth.ApiKeyAuthenticator;
import com.telemetry.gateway.publish.EventPublisher;
import com.telemetry.gateway.validation.BatchValidator;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

/** Creates the gateway's building blocks from {@link GatewayProperties}. */
@Configuration
public class GatewayConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    ApiKeyAuthenticator apiKeyAuthenticator(GatewayProperties props) {
        return new ApiKeyAuthenticator(props.agents().stream()
                .map(a -> new AgentCredential(a.agentId(), a.apiKeySha256(), a.allowedServices()))
                .toList());
    }

    @Bean
    BatchValidator batchValidator(GatewayProperties props, Clock clock) {
        return new BatchValidator(clock, props.maxEventAge(), props.maxClockSkew());
    }

    /**
     * Kafka producer settings (FR-ING-07). Durability first: acks=all and the idempotent producer, so a retry
     * inside the client never writes a record twice. The timeouts are short on purpose: if Kafka is down the
     * agent should get 503 within seconds and retry later (spec 6.8), not wait the 2-minute client default.
     */
    @Bean(destroyMethod = "close")
    KafkaSender<String, byte[]> kafkaSender(
            GatewayProperties props, @Value("${spring.application.name}") String appName) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.kafka().bootstrapServers());
        config.put(ProducerConfig.CLIENT_ID_CONFIG, appName);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                5); // highest value that keeps ordering with idempotence
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
        config.putAll(props.kafka().producer());
        return KafkaSender.create(SenderOptions.<String, byte[]>create(config));
    }

    @Bean
    EventPublisher eventPublisher(
            KafkaSender<String, byte[]> sender,
            ObjectMapper mapper,
            GatewayProperties props,
            @Value("${spring.application.name}") String appName,
            @Value("${gateway.version:0.1.0}") String version,
            Clock clock) {
        return new EventPublisher(
                sender,
                mapper,
                props.kafka().rawTopic(),
                props.kafka().rejectedTopic(),
                appName + "/" + version,
                clock);
    }
}
