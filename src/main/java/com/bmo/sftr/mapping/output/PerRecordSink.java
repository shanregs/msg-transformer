package com.bmo.sftr.mapping.output;

import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Write mode {@code PER_RECORD}: opens the output file, appends the row, and closes the file again,
 * for every row. Nothing is held in memory or left open between rows, so each row is on disk when
 * {@link #append} returns. The header is written only when the file is new or empty.
 */
public class PerRecordSink implements ReportSink {

    private final SftrCsvWriter csvWriter;
    private final RecordProcessor processor;
    private final Path outputFile;

    public PerRecordSink(SftrCsvWriter csvWriter, RecordProcessor processor, Path outputFile) {
        this.csvWriter = csvWriter;
        this.processor = processor;
        this.outputFile = outputFile;
    }

    @Override
    public void append(SftrTransactionReport report) throws IOException {
        try (SftrCsvWriter.Session csv = csvWriter.openAppend(outputFile, processor.columnDefs())) {
            csv.write(report);
        }
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
