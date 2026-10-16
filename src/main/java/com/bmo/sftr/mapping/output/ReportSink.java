package com.bmo.sftr.mapping.output;

import com.bmo.sftr.mapping.model.SftrTransactionReport;
import java.io.IOException;

/**
 * Decides when mapped rows reach the output file. Called by one thread at a time; rows must come out
 * in the order {@link #append} was called.
 */
public interface ReportSink extends AutoCloseable {

    /** Accepts one row. When it is written (now or later) is up to the implementation. */
    void append(SftrTransactionReport report) throws IOException;

    /** Writes everything accepted so far. */
    void flush() throws IOException;

    @Override
    void close() throws IOException;
}
