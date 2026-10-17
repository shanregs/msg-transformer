package com.bmo.sftr.mapping.publisher;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

/**
 * The two beans the publisher route calls on every tick: {@link #next} puts the next message in the body,
 * {@link #afterSend} counts it and ends the run once a limit is reached (message count or duration).
 * When the run ends the application exits.
 */
@Component("messagePublisher")
@ConditionalOnProperty(name = "sftr.publisher.enabled", havingValue = "true")
public class MessagePublisher {

    private static final Logger log = LoggerFactory.getLogger(MessagePublisher.class);

    private final PublisherProperties props;
    private final TradeMessageGenerator generator;
    private final Clock clock;
    private final Runnable onFinish;

    private final AtomicLong sent = new AtomicLong();
    private final AtomicBoolean finished = new AtomicBoolean();
    private volatile Instant started;
    private volatile Instant deadline;
    private ScheduledExecutorService deadlineTimer;

    @Autowired
    public MessagePublisher(PublisherProperties props, TradeMessageGenerator generator,
                            ConfigurableApplicationContext context) {
        this(props, generator, Clock.systemUTC(), () -> exitInBackground(context));
    }

    MessagePublisher(PublisherProperties props, TradeMessageGenerator generator, Clock clock, Runnable onFinish) {
        this.props = props;
        this.generator = generator;
        this.clock = clock;
        this.onFinish = onFinish;
    }

    /** Route step 1. Sets the body to the next message, or stops the route if the run is over. */
    public void next(Exchange exchange) {
        if (finished.get()) {
            exchange.setRouteStop(true);
            return;
        }
        if (started == null) start();
        if (deadline != null && !clock.instant().isBefore(deadline)) {
            finish();
            exchange.setRouteStop(true);
            return;
        }
        exchange.getIn().setBody(generator.message(sent.get() + 1));
    }

    /** Route step 2, after the message is on Kafka. */
    public void afterSend(Exchange exchange) {
        long n = sent.incrementAndGet();
        if (props.hasMaxMessages() && n >= props.maxMessages()) finish();
    }

    public long sent() {
        return sent.get();
    }

    private synchronized void start() {
        if (started != null) return;
        started = clock.instant();
        log.info("Publishing to {}: rate {}/min, max messages {}, duration {}", props.topic(), props.ratePerMinute(),
                props.hasMaxMessages() ? props.maxMessages() : "unlimited",
                props.hasDuration() ? props.duration() : "unlimited");
        if (props.hasDuration()) {
            deadline = started.plus(props.duration());
            // end on time even when the next tick is far away (a slow rate)
            deadlineTimer = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "publisher-deadline");
                t.setDaemon(true);
                return t;
            });
            deadlineTimer.schedule(this::finish, props.duration().toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void finish() {
        if (!finished.compareAndSet(false, true)) return;
        Duration took = started == null ? Duration.ZERO : Duration.between(started, clock.instant());
        log.info("Publisher finished: {} messages in {} ms", sent.get(), took.toMillis());
        if (deadlineTimer != null) deadlineTimer.shutdown();
        onFinish.run();
    }

    private static void exitInBackground(ConfigurableApplicationContext context) {
        // not on the Camel timer thread: closing the context stops the route that is running this code
        Thread.ofPlatform().name("publisher-exit").start(() -> SpringApplication.exit(context, () -> 0));
    }
}
