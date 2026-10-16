package com.bmo.sftr.mapping.service;

import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.model.SftrConstants;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import com.bmo.sftr.mapping.output.PerRecordSink;
import com.bmo.sftr.mapping.output.RejectWriter;
import com.bmo.sftr.mapping.output.ReportSink;
import com.bmo.sftr.mapping.output.SftrCsvWriter;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import java.io.IOException;
import java.nio.file.Path;
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

    @Autowired
    public RecordPipeline(MessageTransformer transformer, ReportSink sink, RejectWriter rejects) {
        this.transformer = transformer;
        this.sink = sink;
        this.rejects = rejects;
    }

    /** Builds the collaborators by hand, with the settings the Spring beans would read from configuration. */
    RecordPipeline(RefDataClient refData, RecordProcessor processor, SftrCsvWriter csvWriter,
                   SftrConstants constants, ObjectMapper mapper, String outputFile, String rejectsFile,
                   String configuredFileDate) {
        this(new MessageTransformer(refData, processor, constants, mapper, configuredFileDate),
                new PerRecordSink(csvWriter, processor, Path.of(outputFile)),
                new RejectWriter(outputFile, rejectsFile));
    }

    /**
     * @param json      the message value as received
     * @param sourceRef where it came from (for example topic-partition@offset); goes to the rejects file
     */
    public Outcome handle(String json, String sourceRef) throws IOException {
        SftrTransactionReport report;
        try {
            report = transformer.transform(json);
        } catch (RuntimeException e) {                 // includes reference data failures (the client already retries)
            rejects.write(sourceRef, RejectWriter.summarize(e));
            return Outcome.REJECTED;
        }
        sink.append(report);
        return Outcome.WRITTEN;
    }

    /** For tests that build the pipeline by hand; Spring closes the sink and the reject writer itself. */
    void close() throws IOException {
        sink.close();
        rejects.close();
    }
}
