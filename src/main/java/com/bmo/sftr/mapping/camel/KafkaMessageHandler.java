package com.bmo.sftr.mapping.camel;

import com.bmo.sftr.mapping.service.RecordPipeline;
import jakarta.annotation.PreDestroy;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The bean the route in {@code routes/kafka-ingest.camel.yaml} calls for every Kafka message
 * ({@code bean: ref: kafkaMessageHandler, method: handle}). It hands the message to {@link RecordPipeline}.
 *
 * <p>Offsets are not committed automatically. Each message gives {@link RecordPipeline} a callback that runs
 * once its row is on disk; the callback releases the offset to {@link OffsetCommitter}. The next route step,
 * {@link OffsetCommitProcessor}, commits whatever has been released, on the consumer thread.
 *
 * <p>Any error is logged and processing carries on with the next message. Delivery is at least once: after a
 * crash, messages whose rows were written but whose offset was not yet committed are read again.
 */
@Component("kafkaMessageHandler")
public class KafkaMessageHandler {

    static final String ROUTE_ID = "sftr-kafka-ingest";

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageHandler.class);

    private final RecordPipeline pipeline;
    private final OffsetCommitter committer;
    private final CamelContext camelContext;

    public KafkaMessageHandler(RecordPipeline pipeline, OffsetCommitter committer, CamelContext camelContext) {
        this.pipeline = pipeline;
        this.committer = committer;
        this.camelContext = camelContext;
    }

    /** Runs on the Camel Kafka consumer thread. Never throws. */
    public void handle(Exchange exchange) {
        String topic = exchange.getIn().getHeader(KafkaConstants.TOPIC, String.class);
        Integer partition = exchange.getIn().getHeader(KafkaConstants.PARTITION, Integer.class);
        Long offset = exchange.getIn().getHeader(KafkaConstants.OFFSET, Long.class);
        KafkaManualCommit manualCommit = exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        String ref = topic + "-" + partition + "@" + offset;
        try {
            pipeline.handle(exchange.getIn().getBody(String.class), ref,
                    () -> committer.release(topic + "-" + partition, offset, manualCommit::commit));
        } catch (Exception e) {
            log.error("Failed to process {}; skipping to the next message", ref, e);
        }
    }

    /** Stops the consumer before the pipeline and sink are closed, so no message arrives at a closed sink. */
    @PreDestroy
    void stopConsuming() {
        try {
            if (camelContext.getRoute(ROUTE_ID) != null) {
                camelContext.getRouteController().stopRoute(ROUTE_ID);
            }
        } catch (Exception e) {
            log.warn("Could not stop route {} cleanly", ROUTE_ID, e);
        }
    }
}
