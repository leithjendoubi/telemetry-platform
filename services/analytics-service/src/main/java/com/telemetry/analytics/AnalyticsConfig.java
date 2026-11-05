package com.telemetry.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.analytics.ingest.ScoredEventListener;
import com.telemetry.analytics.ingest.ScoredEventParser;
import com.telemetry.analytics.ingest.TelemetryEventWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class AnalyticsConfig {

    @Bean
    ScoredEventParser scoredEventParser(ObjectMapper mapper) {
        return new ScoredEventParser(mapper);
    }

    @Bean
    TelemetryEventWriter telemetryEventWriter(JdbcTemplate jdbc, TransactionTemplate tx) {
        return new TelemetryEventWriter(jdbc, tx);
    }

    @Bean
    ScoredEventListener scoredEventListener(
            ScoredEventParser parser,
            TelemetryEventWriter writer,
            KafkaTemplate<byte[], byte[]> dlqTemplate,
            @Value("${analytics.kafka.dlq-topic}") String dlqTopic,
            @Value("${spring.application.name}/${analytics.version}") String producerName) {
        return new ScoredEventListener(parser, writer, dlqTemplate, dlqTopic, producerName);
    }

    /**
     * When a batch fails (database down, DLQ unreachable), retry the SAME batch every 2 s, forever, without
     * committing. Poison records never reach this point: the listener sends them to the DLQ itself.
     * Spring Boot plugs this bean into the listener container automatically.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(@Value("${analytics.kafka.retry-interval-ms}") long retryMs) {
        return new DefaultErrorHandler(new FixedBackOff(retryMs, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
