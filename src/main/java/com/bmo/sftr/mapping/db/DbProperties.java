package com.bmo.sftr.mapping.db;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Settings under {@code sftr.db}.
 *
 * @param event    which rows are processed; only rows with this event are read
 * @param pageSize how many rows are fetched per query (memory use, not the batch size of the CSV writes)
 */
@ConfigurationProperties(prefix = "sftr.db")
public record DbProperties(
        @DefaultValue("RECEIVED") TradeEvent event,
        @DefaultValue("500") int pageSize) {

    public DbProperties {
        if (event == null) event = TradeEvent.RECEIVED;
        if (pageSize < 1) throw new IllegalArgumentException("sftr.db.page-size must be at least 1");
    }
}
