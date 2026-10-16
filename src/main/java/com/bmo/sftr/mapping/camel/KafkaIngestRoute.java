package com.bmo.sftr.mapping.camel;

import com.bmo.sftr.mapping.service.RecordPipeline;
import jakarta.annotation.PreDestroy;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

/**
 * Receives trade messages from Kafka through Camel and hands each to {@link RecordPipeline}.
 *
 * <p>One consumer, so messages are handled one at a time in partition order, on the Camel consumer thread.
 * Offsets are not committed automatically. Each message gives {@link RecordPipeline} a callback that runs
 * once its row is on disk; the callback releases the offset to {@link OffsetCommitter}, and this route
 * commits whatever has been released right after every message, on the consumer thread.
 *
 * <p>Any error is logged and processing carries on with the next message (as before). Delivery is at least
 * once: after a crash, messages whose rows were written but whose offset was not yet committed are read again.
 */
@Component
@ConditionalOnExpression("'${sftr.ingest:camel}' == 'camel' and '${sftr.kafka.enabled:true}' == 'true'")
public class KafkaIngestRoute extends RouteBuilder {

    static final String ROUTE_ID = "sftr-kafka-ingest";

    private static final Logger log = LoggerFactory.getLogger(KafkaIngestRoute.class);

    private final RecordPipeline pipeline;
    private final OffsetCommitter committer;
    private final String endpoint;

    public KafkaIngestRoute(RecordPipeline pipeline, OffsetCommitter committer,
                            @Value("${sftr.kafka.topic}") String topic,
                            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String brokers,
                            @Value("${spring.kafka.consumer.group-id:sftr-mapping-engine}") String groupId,
                            @Value("${spring.kafka.consumer.auto-offset-reset:earliest}") String autoOffsetReset) {
        this.pipeline = pipeline;
        this.committer = committer;
        this.endpoint = "kafka:" + topic + "?brokers=" + brokers + "&groupId=" + groupId
                + "&autoOffsetReset=" + autoOffsetReset
                + "&consumersCount=1&maxPollRecords=1"
                + "&allowManualCommit=true&autoCommitEnable=false";
    }

    @Override
    public void configure() {
        from(endpoint).routeId(ROUTE_ID).process(this::handle);
    }

    /** Runs on the Camel Kafka consumer thread. */
    void handle(Exchange exchange) {
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
        committer.commitReleased();         // this is the consumer thread: commit everything now safe on disk
    }

    /** Stops the consumer before the pipeline and sink are closed, so no message arrives at a closed sink. */
    @PreDestroy
    void stopConsuming() {
        try {
            if (getContext() != null && getContext().getRouteController().getRouteStatus(ROUTE_ID) != null) {
                getContext().getRouteController().stopRoute(ROUTE_ID);
            }
        } catch (Exception e) {
            log.warn("Could not stop route {} cleanly", ROUTE_ID, e);
        }
    }
}
