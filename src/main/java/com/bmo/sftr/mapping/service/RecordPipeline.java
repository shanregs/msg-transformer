package com.bmo.sftr.mapping.service;

import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.model.SftrConstants;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import com.bmo.sftr.mapping.output.OutputFileResolver;
import com.bmo.sftr.mapping.output.OutputProperties;
import com.bmo.sftr.mapping.output.OutputProperties.Rollover;
import com.bmo.sftr.mapping.output.OutputProperties.WriteMode;
import com.bmo.sftr.mapping.output.PerRecordSink;
import com.bmo.sftr.mapping.output.ReportDay;
import com.bmo.sftr.mapping.output.ReportSink;
import com.bmo.sftr.mapping.output.SftrCsvWriter;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.concurrent.locks.ReentrantLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

/**
 * Handles ONE record at a time, on the calling thread: {@link MessageTransformer} builds the row and the
 * {@link ReportSink} writes it (or buffers it). Rows therefore appear in exactly the order records are handed in.
 *
 * Calls are serialised by a lock, because the sink owns the output file: the database run and the optional
 * REST test endpoint can both call in, and rows still come out in the order calls were made.
 *
 * <ul>
 *   <li>Returns normally: the row was handed to the sink (with a batch write mode it may still be waiting
 *       until the batch is full or {@link #flush()} is called).</li>
 *   <li>{@link RecordProcessingException}: the record could not be processed (malformed JSON, reference data
 *       failure after its own retries, mapping error). Nothing is written; the caller logs it and moves on.</li>
 *   <li>{@link IOException}: the output file could not be written; the caller logs it and moves on.</li>
 * </ul>
 */
@Service
public class RecordPipeline {

    private final MessageTransformer transformer;
    private final ReportSink sink;
    private final ReportDay reportDay;
    private final ReentrantLock lock = new ReentrantLock();

    @Autowired
    public RecordPipeline(MessageTransformer transformer, ReportSink sink, ReportDay reportDay) {
        this.transformer = transformer;
        this.sink = sink;
        this.reportDay = reportDay;
    }

    /** Builds the collaborators by hand, one file with the configured name (no daily files). */
    RecordPipeline(RefDataClient refData, RecordProcessor processor, SftrCsvWriter csvWriter,
                   SftrConstants constants, ObjectMapper mapper, String outputFile, String configuredFileDate) {
        this(refData, processor, csvWriter, constants, mapper, new OutputProperties(outputFile,
                configuredFileDate, Rollover.NONE, ZoneId.of("Asia/Kolkata"), WriteMode.PER_RECORD, 10,
                Duration.ofMinutes(5), 1000), Clock.system(ZoneId.of("Asia/Kolkata")));
    }

    /** Builds the collaborators by hand from the given settings and clock. */
    RecordPipeline(RefDataClient refData, RecordProcessor processor, SftrCsvWriter csvWriter,
                   SftrConstants constants, ObjectMapper mapper, OutputProperties props, Clock clock) {
        this(new MessageTransformer(refData, processor, constants, mapper),
                new PerRecordSink(csvWriter, new OutputFileResolver(props)), new ReportDay(props, clock));
    }

    /**
     * @param json      the record's JSON as stored or received
     * @param sourceRef where it came from (for example {@code trade:42}); named in the exception, never the content
     * @throws RecordProcessingException the record cannot be processed
     * @throws IOException               the output file cannot be written
     */
    public void handle(String json, String sourceRef) throws IOException {
        lock.lock();
        try {
            LocalDate day = reportDay.today();         // once per record: the row's date and its file agree
            SftrTransactionReport report;
            try {
                report = transformer.transform(json, day);
            } catch (RuntimeException e) {             // includes reference data failures (the client already retries)
                throw new RecordProcessingException(sourceRef, e);
            }
            sink.append(report, day);
        } finally {
            lock.unlock();
        }
    }

    /** Writes any rows still waiting in the sink (end of a run, shutdown). */
    public void flush() throws IOException {
        lock.lock();
        try {
            sink.flush();
        } finally {
            lock.unlock();
        }
    }

    /** For tests that build the pipeline by hand; Spring closes the sink itself. */
    void close() throws IOException {
        sink.close();
    }
}
