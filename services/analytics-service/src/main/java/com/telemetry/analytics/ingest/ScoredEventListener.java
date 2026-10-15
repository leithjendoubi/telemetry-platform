package com.telemetry.analytics.ingest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.telemetry.analytics.ingest.ScoredEventParser.PoisonRecordException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * FR-ANA-01: consume telemetry.scored.v1 in batches (at most 500 records, at most ~200 ms of waiting, see
 * application.yml) and store them.
 *
 * <p>Order inside one batch, and why:
 * <ol>
 *   <li>parse every record; broken ones are sent to the DLQ and Kafka must acknowledge that send;</li>
 *   <li>insert the good ones in one database transaction;</li>
 *   <li>return. Only now does Spring Kafka commit the offsets (ack-mode BATCH).</li>
 * </ol>
 * If step 1 or 2 fails, the method throws, nothing is committed, and the error handler retries the same batch
 * every few seconds until the database is back (spec 6.8: "Analytics stops committing offsets; lag grows").
 * A crash after step 2 replays the batch; ON CONFLICT DO NOTHING absorbs the repeats.
 */
public class ScoredEventListener {

    private static final Logger log = LoggerFactory.getLogger(ScoredEventListener.class);

    private final ScoredEventParser parser;
    private final TelemetryEventWriter writer;
    private final KafkaTemplate<byte[], byte[]> dlqTemplate;
    private final String dlqTopic;
    private final String producerName;

    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong inserted = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong deadLettered = new AtomicLong();
    private final AtomicLong batches = new AtomicLong();
    private long lastLoggedConsumed = -1;

    public ScoredEventListener(ScoredEventParser parser, TelemetryEventWriter writer, KafkaTemplate<byte[], byte[]> dlqTemplate,
                               String dlqTopic, String producerName) {
        this.parser = parser;
        this.writer = writer;
        this.dlqTemplate = dlqTemplate;
        this.dlqTopic = dlqTopic;
        this.producerName = producerName;
    }

    @KafkaListener(id = "analytics-writer", topics = "${analytics.kafka.scored-topic}", batch = "true")
    public void onBatch(List<ConsumerRecord<String, byte[]>> records) throws Exception {
        List<TelemetryRow> rows = new ArrayList<>(records.size());
        int poison = 0;
        for (ConsumerRecord<String, byte[]> record : records) {
            try {
                rows.add(parser.parse(record.value()));
            } catch (PoisonRecordException e) {
                deadLetter(record, e.getMessage());
                poison++;
            }
        }
        int fresh = writer.write(rows);

        consumed.addAndGet(records.size());
        inserted.addAndGet(fresh);
        duplicates.addAndGet(rows.size() - fresh);
        deadLettered.addAndGet(poison);
        batches.incrementAndGet();
        log.debug("batch of {}: {} inserted, {} duplicates, {} dead-lettered", records.size(), fresh, rows.size() - fresh, poison);
    }

    private void deadLetter(ConsumerRecord<String, byte[]> record, String reason) throws Exception {
        log.warn("poison record {}[{}]@{} -> {}: {}", record.topic(), record.partition(), record.offset(), dlqTopic, reason);
        ProducerRecord<byte[], byte[]> out = new ProducerRecord<>(dlqTopic, null,
                record.key() == null ? null : record.key().getBytes(StandardCharsets.UTF_8), record.value());
        for (Header h : record.headers()) {
            out.headers().add(h);
        }
        out.headers().add("dlq-error", reason.getBytes(StandardCharsets.UTF_8));
        out.headers().add("dlq-attempts", "1".getBytes(StandardCharsets.UTF_8));
        out.headers().add("dlq-source-topic", record.topic().getBytes(StandardCharsets.UTF_8));
        out.headers().add("dlq-source-partition", Integer.toString(record.partition()).getBytes(StandardCharsets.UTF_8));
        out.headers().add("dlq-source-offset", Long.toString(record.offset()).getBytes(StandardCharsets.UTF_8));
        out.headers().add("producer", producerName.getBytes(StandardCharsets.UTF_8));
        dlqTemplate.send(out).get(10, TimeUnit.SECONDS); // must be stored before the offsets are committed
    }

    /** One log line every 10 s while records are flowing. */
    @Scheduled(fixedDelay = 10_000)
    void logStats() {
        long now = consumed.get();
        if (now != lastLoggedConsumed) {
            log.info("stats consumed={} inserted={} duplicates={} dead_lettered={} batches={}",
                    now, inserted.get(), duplicates.get(), deadLettered.get(), batches.get());
            lastLoggedConsumed = now;
        }
    }
}
