package com.bmo.sftr.mapping.output;

import com.bmo.sftr.mapping.model.SftrTransactionReport;
import java.io.IOException;
import java.time.LocalDate;

/**
 * Write mode {@code PER_RECORD}: opens the output file, appends the row, and closes the file again,
 * for every row. Nothing is held in memory or left open between rows, so each row is on disk when
 * {@link #append} returns. The header is written only when the file is new or empty, which also
 * makes the first row of a new day start that day's file.
 */
public class PerRecordSink implements ReportSink {

    private final SftrCsvWriter csvWriter;
    private final OutputFileResolver files;

    public PerRecordSink(SftrCsvWriter csvWriter, OutputFileResolver files) {
        this.csvWriter = csvWriter;
        this.files = files;
    }

    @Override
    public void append(SftrTransactionReport report, LocalDate day, Runnable whenDurable) throws IOException {
        try (SftrCsvWriter.Session csv = csvWriter.openAppend(files.report(day))) {
            csv.write(report);
        }
        whenDurable.run();
    }

    @Override
    public void whenDurable(Runnable action) {
        action.run();                       // nothing is ever waiting
    }

    @Override
    public void flush() {
        // nothing is buffered
    }

    @Override
    public void close() {
        // nothing is kept open
    }
}
