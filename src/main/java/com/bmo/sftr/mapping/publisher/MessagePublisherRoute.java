package com.bmo.sftr.mapping.publisher;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Timer to Kafka. The timer period comes from {@code sftr.publisher.rate-per-minute}, which is why this route
 * is Java and not YAML: the period is computed (60000 / rate). Limits are applied by {@link MessagePublisher}.
 *
 * <p>Run it with {@code --spring.profiles.active=publisher}.
 */
@Component
@ConditionalOnProperty(name = "sftr.publisher.enabled", havingValue = "true")
public class MessagePublisherRoute extends RouteBuilder {

    static final String ROUTE_ID = "sftr-publisher";

    private final MessagePublisher publisher;
    private final String timerUri;
    private final String targetUri;

    @Autowired
    public MessagePublisherRoute(MessagePublisher publisher, PublisherProperties props,
                                 @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String brokers) {
        this(props, publisher, "kafka:" + props.topic() + "?brokers=" + brokers);
    }

    /** Lets a test replace the Kafka endpoint. */
    MessagePublisherRoute(PublisherProperties props, MessagePublisher publisher, String targetUri) {
        this.publisher = publisher;
        this.timerUri = "timer:sftr-publisher?period=" + props.periodMillis() + "&delay=0&fixedRate=true";
        this.targetUri = targetUri;
    }

    @Override
    public void configure() {
        from(timerUri).routeId(ROUTE_ID)
                .bean(publisher, "next")
                .to(targetUri)
                .bean(publisher, "afterSend");
    }
}
