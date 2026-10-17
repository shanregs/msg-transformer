package com.bmo.sftr.mapping.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

class MessagePublisherTest {

    /** A clock the test moves by hand. */
    static class MutableClock extends Clock {
        Instant now = Instant.parse("2026-10-17T00:00:00Z");
        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    private static PublisherProperties props(int rate, long max, Duration duration) {
        return new PublisherProperties(true, "t", rate, max, duration,
                List.of("PO", "PI"), List.of("LDNBB01"), List.of("CP1", "CP2", "CP3"));
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final MutableClock clock = new MutableClock();
    private final AtomicInteger finished = new AtomicInteger();

    private MessagePublisher publisher(PublisherProperties p) {
        return new MessagePublisher(p, new TradeMessageGenerator(mapper, p), clock, finished::incrementAndGet);
    }

    private Exchange exchange() {
        return new DefaultExchange(new DefaultCamelContext());
    }

    /** One tick of the route: next, then (if it was not stopped) afterSend. Returns the body or null. */
    private String tick(MessagePublisher mp) {
        Exchange ex = exchange();
        mp.next(ex);
        if (ex.isRouteStop()) return null;
        mp.afterSend(ex);
        return ex.getIn().getBody(String.class);
    }

    @Test
    void periodIsSixtyThousandOverTheRate() {
        assertThat(props(60, 0, Duration.ZERO).periodMillis()).isEqualTo(1000);
        assertThat(props(120, 0, Duration.ZERO).periodMillis()).isEqualTo(500);
        assertThat(props(1, 0, Duration.ZERO).periodMillis()).isEqualTo(60_000);
        assertThat(props(0, 0, Duration.ZERO).periodMillis()).isEqualTo(1);
        assertThat(props(600_000, 0, Duration.ZERO).periodMillis()).isEqualTo(1);
    }

    @Test
    void badSettingsAreRefused() {
        assertThatThrownBy(() -> props(-1, 0, Duration.ZERO)).hasMessageContaining("rate-per-minute");
        assertThatThrownBy(() -> props(1, -1, Duration.ZERO)).hasMessageContaining("max-messages");
        assertThatThrownBy(() -> props(1, 0, Duration.ofSeconds(-1))).hasMessageContaining("duration");
        assertThatThrownBy(() -> new PublisherProperties(true, "t", 1, 0, Duration.ZERO,
                List.of(), List.of("a"), List.of("b"))).hasMessageContaining("trade-types");
    }

    @Test
    void stopsAfterTheFixedCountAndFinishesOnce() {
        MessagePublisher mp = publisher(props(60, 3, Duration.ZERO));
        assertThat(tick(mp)).isNotNull();
        assertThat(tick(mp)).isNotNull();
        assertThat(finished).hasValue(0);
        assertThat(tick(mp)).isNotNull();
        assertThat(finished).hasValue(1);
        assertThat(tick(mp)).isNull();                          // a late tick sends nothing
        assertThat(mp.sent()).isEqualTo(3);
        assertThat(finished).hasValue(1);
    }

    @Test
    void withNoLimitItNeverFinishes() {
        MessagePublisher mp = publisher(props(60, 0, Duration.ZERO));
        for (int i = 0; i < 50; i++) assertThat(tick(mp)).isNotNull();
        assertThat(finished).hasValue(0);
    }

    @Test
    void stopsOnceTheDurationHasPassed() {
        MessagePublisher mp = publisher(props(60, 0, Duration.ofSeconds(10)));
        assertThat(tick(mp)).isNotNull();
        clock.now = clock.now.plusSeconds(9);
        assertThat(tick(mp)).isNotNull();
        assertThat(finished).hasValue(0);
        clock.now = clock.now.plusSeconds(1);                   // exactly 10 s after the first message
        assertThat(tick(mp)).isNull();
        assertThat(finished).hasValue(1);
        assertThat(mp.sent()).isEqualTo(2);
    }

    @Test
    void whicheverLimitComesFirstWins() {
        MessagePublisher mp = publisher(props(60, 2, Duration.ofHours(1)));
        tick(mp);
        tick(mp);
        assertThat(finished).hasValue(1);                       // count hit long before the duration
    }

    @Test
    void messagesRotateThroughTheConfiguredValues() throws Exception {
        PublisherProperties p = props(60, 0, Duration.ZERO);
        TradeMessageGenerator g = new TradeMessageGenerator(mapper, p);

        assertThat(mapper.readTree(g.message(1)).get("counterpartyId").asString()).isEqualTo("CP1");
        assertThat(mapper.readTree(g.message(2)).get("counterpartyId").asString()).isEqualTo("CP2");
        assertThat(mapper.readTree(g.message(4)).get("counterpartyId").asString()).isEqualTo("CP1");
        assertThat(mapper.readTree(g.message(1)).get("trade_type").asString()).isEqualTo("PO");
        assertThat(mapper.readTree(g.message(2)).get("trade_type").asString()).isEqualTo("PI");
        assertThat(mapper.readTree(g.message(3)).get("trade_ref").asString()).isEqualTo("PUB-3");
        assertThat(g.message(7)).isEqualTo(g.message(7));       // repeatable
    }
}
