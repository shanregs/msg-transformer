package com.bmo.sftr.mapping.rest;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Sends one message to a Kafka topic, so that in {@code both} mode the consumer picks it up like any other
 * message. Only present when {@code sftr.rest.enabled=true} (the {@code rest} or {@code both} profile).
 *
 * <pre>
 * curl -X POST "localhost:9090/api/kafka-message" -H "Content-Type: application/json" -d '{"trade_type":"PO",...}'
 * curl -X POST "localhost:9090/api/kafka-message?topic=other-topic&amp;key=k1" -d '{...}'
 * </pre>
 *
 * The topic defaults to {@code sftr.kafka.topic}, the one the consumer listens to. The call waits for the
 * broker's acknowledgement and answers with the partition and offset. The body is sent exactly as received:
 * it is not parsed here, so a bad message reaches the consumer and ends up in the rejects file, which is
 * useful for testing. If Kafka cannot be reached the answer is 503 (in {@code rest} mode there is usually
 * no broker). The message content is never logged or echoed.
 */
@RestController
@RequestMapping("/api")
@ConditionalOnProperty(name = "sftr.rest.enabled", havingValue = "true")
public class KafkaPublishController {

    /** @param offset position of the message in the partition */
    public record Result(String topic, int partition, long offset) {}

    public record Error(String error) {}

    private static final Logger log = LoggerFactory.getLogger(KafkaPublishController.class);

    private final KafkaTemplate<String, String> template;
    private final String defaultTopic;
    private final Duration timeout;

    public KafkaPublishController(KafkaTemplate<String, String> template,
                                  @Value("${sftr.kafka.topic}") String defaultTopic,
                                  @Value("${sftr.rest.publish-timeout:10s}") Duration timeout) {
        this.template = template;
        this.defaultTopic = defaultTopic;
        this.timeout = timeout;
    }

    @PostMapping(path = "/kafka-message", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> publish(@RequestBody(required = false) String body,
                                     @RequestParam(required = false) String topic,
                                     @RequestParam(required = false) String key) {
        if (body == null || body.isBlank()) {
            return ResponseEntity.badRequest().body(new Error("Empty message"));
        }
        String target = topic == null || topic.isBlank() ? defaultTopic : topic.trim();
        try {
            RecordMetadata meta = template.send(target, blankToNull(key), body)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS).getRecordMetadata();
            return ResponseEntity.ok(new Result(meta.topic(), meta.partition(), meta.offset()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return unavailable(target, e);
        } catch (ExecutionException e) {
            return unavailable(target, e.getCause() != null ? e.getCause() : e);
        } catch (TimeoutException | RuntimeException e) {
            return unavailable(target, e);
        }
    }

    private static ResponseEntity<Error> unavailable(String topic, Throwable cause) {
        log.warn("Could not publish to Kafka topic {}: {}", topic, cause.toString());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new Error("Could not publish to Kafka topic '" + topic + "': " + cause.getClass().getSimpleName()));
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }
}
