package com.bmo.sftr.mapping.output;

import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.file.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Keeps the output file open and appends each row as it arrives, flushing after every row by default.
 * This is the behaviour before the write modes were introduced.
 */
@Component
public class AppendingReportSink implements ReportSink {

    private final SftrCsvWriter csvWriter;
    private final RecordProcessor processor;
    private final Path outputFile;
    private final boolean flushEachRecord;

    private SftrCsvWriter.Session csv;

    public AppendingReportSink(SftrCsvWriter csvWriter, RecordProcessor processor,
                               @Value("${sftr.output.file}") String outputFile,
                               @Value("${sftr.output.flush-each-record:true}") boolean flushEachRecord) {
        this.csvWriter = csvWriter;
        this.processor = processor;
        this.outputFile = Path.of(outputFile);
        this.flushEachRecord = flushEachRecord;
    }

    @Override
    public void append(SftrTransactionReport report) throws IOException {
        if (csv == null) csv = csvWriter.openAppend(outputFile, processor.columnDefs());
        csv.write(report);
        if (flushEachRecord) csv.flush();
    }

    @Override
    public void flush() throws IOException {
        if (csv != null) csv.flush();
    }

    @Override
    @PreDestroy
    public void close() throws IOException {
        if (csv != null) {
            csv.close();
            csv = null;
        }
    }
}
