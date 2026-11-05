package com.telemetry.gateway.config;

import java.time.Duration;
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
 * @param retryAfter    value of the Retry-After header on 503 when Kafka or the agent registry cannot answer
 * @param kafka         producer settings
 * @param registry      agent registry settings (agents live in ingestion_db, spec 8.4)
 */
@ConfigurationProperties("gateway")
public record GatewayProperties(
        @DefaultValue("262144") int maxBodyBytes,
        @DefaultValue("100") int maxEvents,
        @DefaultValue("24h") Duration maxEventAge,
        @DefaultValue("5m") Duration maxClockSkew,
        @DefaultValue("5s") Duration retryAfter,
        @DefaultValue Kafka kafka,
        @DefaultValue Registry registry) {

    /**
     * @param cacheTtl      how long a lookup answer is reused: a revocation takes effect within this time (10 s)
     * @param cacheMaxSize  most key hashes kept in the cache
     * @param lookupTimeout a database lookup slower than this fails, and the request gets 503
     */
    public record Registry(
            @DefaultValue("10s") Duration cacheTtl,
            @DefaultValue("10000") long cacheMaxSize,
            @DefaultValue("2s") Duration lookupTimeout) {}

    /**
     * @param bootstrapServers Kafka address: localhost:9092 from Windows, kafka:19092 inside compose
     * @param rawTopic         topic for accepted events (spec 8.3)
     * @param rejectedTopic    topic for rejected events with their reason code (spec 8.3, FR-ING-07)
     * @param producer         extra Kafka producer settings, they override the defaults in KafkaConfig
     */
    public record Kafka(
            @DefaultValue("localhost:9092") String bootstrapServers,
            @DefaultValue("telemetry.raw.v1") String rawTopic,
            @DefaultValue("telemetry.rejected.v1") String rejectedTopic,
            @DefaultValue Map<String, String> producer) {}
}
