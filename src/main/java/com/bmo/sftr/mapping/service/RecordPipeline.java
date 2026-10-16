package com.bmo.sftr.mapping.service;

import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.model.SftrConstants;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import com.bmo.sftr.mapping.output.OutputFileResolver;
import com.bmo.sftr.mapping.output.OutputProperties;
import com.bmo.sftr.mapping.output.OutputProperties.Rollover;
import com.bmo.sftr.mapping.output.OutputProperties.WriteMode;
import com.bmo.sftr.mapping.output.PerRecordSink;
import com.bmo.sftr.mapping.output.RejectWriter;
import com.bmo.sftr.mapping.output.ReportDay;
import com.bmo.sftr.mapping.output.ReportSink;
import com.bmo.sftr.mapping.output.SftrCsvWriter;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

/**
 * Handles ONE message at a time, on the calling thread: {@link MessageTransformer} builds the row,
 * the {@link ReportSink} writes it (or buffers it), and a message that cannot be processed goes to the {@link RejectWriter}.
 * Rows therefore appear in exactly the order messages are handed in.
 *
 * Not thread-safe by design: the sink owns one open output file. The caller must be single-threaded
 * (the Kafka listener runs with concurrency 1).
 *
 * <ul>
 *   <li>Written: the row was handed to the sink (with the default write mode it is on disk before this method returns).</li>
 *   <li>Rejected: the message could not be processed (malformed JSON, reference data failure after
 *       its own retries, mapping error). A line goes to the rejects file and handling carries on.</li>
 *   <li>IOException: the output or rejects file could not be written; the caller logs it and moves on.</li>
 * </ul>
 */
@Service
public class RecordPipeline {

    public enum Outcome { WRITTEN, REJECTED }

    private final MessageTransformer transformer;
    private final ReportSink sink;
    private final RejectWriter rejects;
    private final ReportDay reportDay;

    @Autowired
    public RecordPipeline(MessageTransformer transformer, ReportSink sink, RejectWriter rejects, ReportDay reportDay) {
        this.transformer = transformer;
        this.sink = sink;
        this.rejects = rejects;
        this.reportDay = reportDay;
    }

    /** Builds the collaborators by hand, one file with the configured name (no daily files). */
    RecordPipeline(RefDataClient refData, RecordProcessor processor, SftrCsvWriter csvWriter,
                   SftrConstants constants, ObjectMapper mapper, String outputFile, String rejectsFile,
                   String configuredFileDate) {
        this(refData, processor, csvWriter, constants, mapper, new OutputProperties(outputFile, rejectsFile,
                configuredFileDate, Rollover.NONE, ZoneId.of("Asia/Kolkata"), WriteMode.PER_RECORD, 10,
                Duration.ofMinutes(5), 1000), Clock.system(ZoneId.of("Asia/Kolkata")));
    }

    /** Builds the collaborators by hand from the given settings and clock. */
    RecordPipeline(RefDataClient refData, RecordProcessor processor, SftrCsvWriter csvWriter,
                   SftrConstants constants, ObjectMapper mapper, OutputProperties props, Clock clock) {
        this(new MessageTransformer(refData, processor, constants, mapper),
                new PerRecordSink(csvWriter, processor, new OutputFileResolver(props)),
                new RejectWriter(new OutputFileResolver(props)), new ReportDay(props, clock));
    }

    /**
     * @param json      the message value as received
     * @param sourceRef where it came from (for example topic-partition@offset); goes to the rejects file
     */
    public Outcome handle(String json, String sourceRef) throws IOException {
        return handle(json, sourceRef, () -> { });
    }

    /**
     * As {@link #handle(String, String)}, and runs {@code whenDurable} once the outcome is safely on disk:
     * the row is in the CSV, or the reject line is written and every earlier row is in the CSV too.
     * Kafka uses this to release an offset only when nothing before it can still be lost.
     */
    public Outcome handle(String json, String sourceRef, Runnable whenDurable) throws IOException {
        LocalDate day = reportDay.today();             // once per message: the row's date and its file agree
        SftrTransactionReport report;
        try {
            report = transformer.transform(json, day);
        } catch (RuntimeException e) {                 // includes reference data failures (the client already retries)
            rejects.write(sourceRef, RejectWriter.summarize(e), day);
            sink.whenDurable(whenDurable);
            return Outcome.REJECTED;
        }
        sink.append(report, day, whenDurable);
        return Outcome.WRITTEN;
    }

    /** Writes any rows still waiting in the sink (end of a file run, shutdown). */
    public void flush() throws IOException {
        sink.flush();
    }

    /** For tests that build the pipeline by hand; Spring closes the sink and the reject writer itself. */
    void close() throws IOException {
        sink.close();
        rejects.close();
    }
}
