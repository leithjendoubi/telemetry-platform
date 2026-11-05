package com.telemetry.gateway.config;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Settings under the {@code gateway.} prefix of application.yml.
 *
 * @param maxBodyBytes  largest accepted request body (spec 8.1: 256 KiB), larger gives 413
 * @param maxEvents     most events per batch (spec 8.1: 100), more gives 413
 * @param maxEventAge   events older than this are rejected with TIMESTAMP_OUT_OF_RANGE (spec 8.1: 24 h)
 * @param maxClockSkew  events further in the future than this are rejected the same way (spec 8.1: 5 min)
 * @param retryAfter    value of the Retry-After header on 503 when Kafka cannot take the events
 * @param kafka         producer settings
 * @param agents        the agents allowed to send data. Skeleton only: in week 5 they move to ingestion_db.
 */
@ConfigurationProperties("gateway")
public record GatewayProperties(
        @DefaultValue("262144") int maxBodyBytes,
        @DefaultValue("100") int maxEvents,
        @DefaultValue("24h") Duration maxEventAge,
        @DefaultValue("5m") Duration maxClockSkew,
        @DefaultValue("5s") Duration retryAfter,
        @DefaultValue Kafka kafka,
        @DefaultValue List<AgentEntry> agents) {

    /**
     * @param bootstrapServers Kafka address: localhost:9092 from Windows, kafka:19092 inside compose
     * @param rawTopic         topic for accepted events (spec 8.3)
     * @param producer         extra Kafka producer settings, they override the defaults in KafkaConfig
     */
    public record Kafka(
            @DefaultValue("localhost:9092") String bootstrapServers,
            @DefaultValue("telemetry.raw.v1") String rawTopic,
            @DefaultValue Map<String, String> producer) {}

    /**
     * One registered agent.
     *
     * @param agentId         must equal agent.agentId in the batches it sends
     * @param apiKeySha256    SHA-256 of the API key, 64 hex characters. The key itself is never stored (FR-ING-05).
     * @param allowedServices serviceName values this agent may report on
     */
    public record AgentEntry(String agentId, String apiKeySha256, List<String> allowedServices) {}
}
