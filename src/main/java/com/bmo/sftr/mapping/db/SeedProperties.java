package com.bmo.sftr.mapping.db;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Settings under {@code sftr.db.seed}: fill an empty trade table with sample rows, for local development only.
 * Off by default; the {@code seed} profile turns it on.
 *
 * @param enabled whether {@link TradeSeeder} runs at start-up
 * @param count   how many RECEIVED rows to insert
 */
@ConfigurationProperties(prefix = "sftr.db.seed")
public record SeedProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("10") int count) {

    public SeedProperties {
        if (count < 1) throw new IllegalArgumentException("sftr.db.seed.count must be at least 1");
    }
}
