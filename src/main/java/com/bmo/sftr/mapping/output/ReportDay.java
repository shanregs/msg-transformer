package com.bmo.sftr.mapping.output;

import java.time.Clock;
import java.time.LocalDate;
import org.springframework.stereotype.Component;

/**
 * The date a message belongs to. Taken once per message, so the date written into the row and the file
 * the row goes to cannot disagree around midnight. A configured {@code sftr.output.file-date} replaces
 * today's date; otherwise the date comes from the clock in the configured time zone.
 */
@Component
public class ReportDay {

    private final Clock clock;
    private final LocalDate pinned;

    public ReportDay(OutputProperties props, Clock clock) {
        this.clock = clock;
        this.pinned = props.pinnedDate();
    }

    public LocalDate today() {
        return pinned != null ? pinned : LocalDate.now(clock);
    }
}
