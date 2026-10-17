package com.bmo.sftr.mapping.publisher;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

/** The real timer route with a seda queue standing in for Kafka. */
class MessagePublisherRouteTest {

    private List<String> run(PublisherProperties p, CountDownLatch finished, int expectAtLeast, long waitMs)
            throws Exception {
        MessagePublisher mp = new MessagePublisher(p, new TradeMessageGenerator(new ObjectMapper(), p),
                Clock.systemUTC(), finished::countDown);
        try (DefaultCamelContext camel = new DefaultCamelContext()) {
            camel.addRoutes(new MessagePublisherRoute(p, mp, "seda:out"));
            camel.start();
            ConsumerTemplate in = camel.createConsumerTemplate();
            List<String> got = new ArrayList<>();
            long end = System.currentTimeMillis() + waitMs;
            while (got.size() < expectAtLeast && System.currentTimeMillis() < end) {
                String body = in.receiveBody("seda:out", 200, String.class);
                if (body != null) got.add(body);
            }
            assertThat(finished.await(waitMs, TimeUnit.MILLISECONDS)).isTrue();
            // anything still queued after the limit would show up now
            String extra;
            while ((extra = in.receiveBody("seda:out", 300, String.class)) != null) got.add(extra);
            return got;
        }
    }

    @Test
    void sendsExactlyTheFixedCountThenStops() throws Exception {
        PublisherProperties p = new PublisherProperties(true, "t", 0, 25, Duration.ZERO,
                List.of("PO"), List.of("LDNBB01"), List.of("CP1"));

        List<String> got = run(p, new CountDownLatch(1), 25, 5000);

        assertThat(got).hasSize(25);
        assertThat(got.get(0)).contains("PUB-1");
        assertThat(got.get(24)).contains("PUB-25");
    }

    @Test
    void sendsAtTheConfiguredRateUntilTheDurationEnds() throws Exception {
        // 600 per minute = one every 100 ms, for 1 second
        PublisherProperties p = new PublisherProperties(true, "t", 600, 0, Duration.ofSeconds(1),
                List.of("PO"), List.of("LDNBB01"), List.of("CP1"));

        List<String> got = run(p, new CountDownLatch(1), Integer.MAX_VALUE, 3000);

        assertThat(got.size()).isBetween(7, 11);
    }
}
