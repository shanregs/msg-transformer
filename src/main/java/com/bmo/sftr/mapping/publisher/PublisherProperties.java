package com.bmo.sftr.mapping.publisher;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Settings under {@code sftr.publisher}. The publisher stops at the first limit it reaches; with no limit it
 * runs until the process is stopped.
 *
 * @param enabled         turns the publisher route on (the {@code publisher} profile does this)
 * @param topic           Kafka topic to publish to
 * @param ratePerMinute   messages per minute; 0 means as fast as possible
 * @param maxMessages     stop after this many messages; 0 means no limit
 * @param duration        stop after this long, counted from the first message; 0 means no limit
 * @param tradeTypes      trade_type values, used in turn
 * @param legalEntities   bmoLegalEntity values, used in turn
 * @param counterpartyIds counterpartyId values, used in turn
 */
@ConfigurationProperties(prefix = "sftr.publisher")
public record PublisherProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("trade-messages") String topic,
        @DefaultValue("60") int ratePerMinute,
        @DefaultValue("0") long maxMessages,
        @DefaultValue("0s") Duration duration,
        @DefaultValue({"PO", "PI", "RP", "RV"}) List<String> tradeTypes,
        @DefaultValue({"LDNBB01", "TORBB02"}) List<String> legalEntities,
        @DefaultValue({"CP1", "CP2", "CP3"}) List<String> counterpartyIds) {

    public PublisherProperties {
        if (ratePerMinute < 0) throw new IllegalArgumentException("sftr.publisher.rate-per-minute must be 0 or more");
        if (maxMessages < 0) throw new IllegalArgumentException("sftr.publisher.max-messages must be 0 or more");
        if (duration == null || duration.isNegative()) {
            throw new IllegalArgumentException("sftr.publisher.duration must be 0 or more");
        }
        require(tradeTypes, "trade-types");
        require(legalEntities, "legal-entities");
        require(counterpartyIds, "counterparty-ids");
    }

    /** Milliseconds between messages: 60000 / rate, at least 1. Rate 0 gives 1. */
    public long periodMillis() {
        return ratePerMinute == 0 ? 1 : Math.max(1, 60_000L / ratePerMinute);
    }

    public boolean hasMaxMessages() {
        return maxMessages > 0;
    }

    public boolean hasDuration() {
        return !duration.isZero();
    }

    private static void require(List<String> list, String key) {
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException("sftr.publisher." + key + " must not be empty");
        }
    }
}
