package com.telemetry.gateway.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Ticker;
import com.telemetry.gateway.metrics.GatewayMetrics;
import com.telemetry.gateway.publish.EventPublisher;
import com.telemetry.gateway.ratelimit.AgentRateLimiter;
import com.telemetry.gateway.ratelimit.InFlightLimiter;
import com.telemetry.gateway.registry.AgentRegistry;
import com.telemetry.gateway.registry.R2dbcAgentLookup;
import com.telemetry.gateway.validation.BatchValidator;
import io.github.bucket4j.TimeMeter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

/** Creates the gateway's building blocks from {@link GatewayProperties}. */
@Configuration
public class GatewayConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    /** Agents from ingestion_db (R2DBC), behind a bounded 10 s cache (FR-ING-05, spec 8.4). */
    @Bean
    AgentRegistry agentRegistry(DatabaseClient databaseClient, GatewayProperties props) {
        GatewayProperties.Registry settings = props.registry();
        return new AgentRegistry(
                new R2dbcAgentLookup(databaseClient, settings.lookupTimeout()),
                settings.cacheTtl(),
                settings.cacheMaxSize(),
                Ticker.systemTicker());
    }

    /** Per-agent token buckets (FR-ING-06); idle buckets go after 10 min, at most 10,000 agents. */
    @Bean
    AgentRateLimiter agentRateLimiter() {
        return new AgentRateLimiter(Duration.ofMinutes(10), 10_000, TimeMeter.SYSTEM_NANOTIME);
    }

    @Bean
    InFlightLimiter inFlightLimiter(GatewayProperties props) {
        return new InFlightLimiter(props.maxInFlight());
    }

    /** The gateway metrics of spec 12.2 (FR-ING-09), exported at /metrics. */
    @Bean
    GatewayMetrics gatewayMetrics(MeterRegistry registry, InFlightLimiter inFlight) {
        return new GatewayMetrics(registry, inFlight);
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
        KafkaSender<String, byte[]> sender = KafkaSender.create(SenderOptions.<String, byte[]>create(config));
        // FR-ING-02: reactor-kafka creates the producer lazily, on the first send, i.e. on a Netty event-loop
        // thread; creating it reads files and starts threads (found by BlockHound on 2026-11-05). Create it here, at
        // start-up. This does not contact the broker, so the gateway still starts while Kafka is down.
        sender.doOnProducer(producer -> producer.metrics().size()).block(Duration.ofSeconds(30));
        return sender;
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
