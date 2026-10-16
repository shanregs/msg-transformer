package com.bmo.sftr.mapping.output;

import com.bmo.sftr.mapping.model.SftrTransactionReport;
import java.io.IOException;
import java.time.LocalDate;

/**
 * Decides when mapped rows reach the output file. Called by one thread at a time; rows must come out
 * in the order {@link #append} was called.
 */
public interface ReportSink extends AutoCloseable {

    /**
     * Accepts one row that belongs to {@code day} (the file for that day, when daily files are on).
     * When it is written (now or later) is up to the implementation. {@code whenDurable} runs once this row
     * is in the file, on whichever thread writes it; it must be quick and must not call back into the sink.
     */
    void append(SftrTransactionReport report, LocalDate day, Runnable whenDurable) throws IOException;

    default void append(SftrTransactionReport report, LocalDate day) throws IOException {
        append(report, day, () -> { });
    }

    /**
     * Runs {@code action} once every row accepted so far is in the file; at once if none is waiting.
     * Used for messages that produce no row (rejects) so their offsets are not released ahead of earlier rows.
     */
    void whenDurable(Runnable action);

    /** Writes everything accepted so far. */
    void flush() throws IOException;

    @Override
    void close() throws IOException;
}
