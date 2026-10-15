package com.bmo.sftr.mapping.kafka;

import com.bmo.sftr.mapping.service.RecordPipeline;
import java.io.IOException;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Consumes messages one at a time, in partition order, with a single consumer thread.
 *
 * <p>The offset is acknowledged only after the row (or the reject line) is on disk, so a crash
 * re-delivers the last message (at-least-once). If a message cannot be handled right now, the
 * exception reaches the error handler, which seeks back to the same offset and retries it forever
 * after the back-off. The partition stays blocked, so nothing later can overtake it.
 *
 * <p>Global order is only guaranteed for a topic with ONE partition. With several partitions the
 * order holds per partition.
 */
@Component
@ConditionalOnProperty(name = "sftr.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaRecordListener {

    private final RecordPipeline pipeline;

    public KafkaRecordListener(RecordPipeline pipeline) {
        this.pipeline = pipeline;
    }

    @KafkaListener(topics = "${sftr.kafka.topic}", concurrency = "1")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) throws IOException {
        pipeline.handle(record.value(), record.topic() + "-" + record.partition() + "@" + record.offset());
        ack.acknowledge();
    }

    /** Retry the failing record in place, indefinitely, instead of skipping it. */
    @Bean
    CommonErrorHandler kafkaErrorHandler(@Value("${sftr.kafka.retry-backoff:5s}") Duration backoff) {
        return new DefaultErrorHandler(new FixedBackOff(backoff.toMillis(), FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
