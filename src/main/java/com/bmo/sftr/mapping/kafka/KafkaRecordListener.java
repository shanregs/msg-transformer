package com.bmo.sftr.mapping.kafka;

import com.bmo.sftr.mapping.service.RecordPipeline;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.bmo.sftr.mapping.output.OutputProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes messages one at a time, in partition order, with a single consumer thread.
 *
 * <p>The offset is acknowledged after the message has been handled. Any error (bad message,
 * reference data failure, write failure) is logged and the message is skipped, so processing
 * always continues with the next message. Retries for reference data live in the client.
 *
 * <p>Global order is only guaranteed for a topic with ONE partition. With several partitions the
 * order holds per partition.
 */
@Component
@ConditionalOnExpression("'${sftr.ingest:camel}' == 'spring-kafka' and '${sftr.kafka.enabled:true}' == 'true'")
public class KafkaRecordListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaRecordListener.class);

    private final RecordPipeline pipeline;

    /**
     * This listener acknowledges right after handling, so it is only safe when each row is written at once.
     * The batch write modes need the Camel route, which releases offsets after the write.
     */
    @Autowired
    public KafkaRecordListener(RecordPipeline pipeline, OutputProperties output) {
        this(pipeline);
        if (output.writeMode() != OutputProperties.WriteMode.PER_RECORD) {
            throw new IllegalStateException("sftr.output.write-mode " + output.writeMode()
                    + " needs sftr.ingest=camel: the spring-kafka listener acknowledges before the batch is written");
        }
    }

    KafkaRecordListener(RecordPipeline pipeline) {
        this.pipeline = pipeline;
    }

    @KafkaListener(topics = "${sftr.kafka.topic}", concurrency = "1")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String ref = record.topic() + "-" + record.partition() + "@" + record.offset();
        try {
            pipeline.handle(record.value(), ref);
        } catch (Exception e) {
            log.error("Failed to process {}; skipping to the next message", ref, e);
        }
        ack.acknowledge();
    }
}
