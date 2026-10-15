package com.bmo.sftr.mapping.service;

import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.model.ProcessingContext;
import com.bmo.sftr.mapping.model.SftrConstants;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import com.bmo.sftr.mapping.output.SftrCsvWriter;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import jakarta.annotation.PreDestroy;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpStatusCodeException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Handles ONE message at a time, on the calling thread, and appends its row to the output file.
 * Rows therefore appear in exactly the order messages are handed in.
 *
 * Not thread-safe by design: it owns one open output file. The caller must be single-threaded
 * (the Kafka listener runs with concurrency 1).
 *
 * <ul>
 *   <li>Written: the row is appended (and flushed, by default) before this method returns.</li>
 *   <li>Rejected: the message itself is bad (malformed JSON, 4xx from reference data, mapping error).
 *       A line goes to the rejects file and handling carries on.</li>
 *   <li>Exception: the message is fine but could not be handled now (reference data unavailable,
 *       output not writable). Nothing was written; the caller must retry the same message before
 *       moving on, otherwise the order would change.</li>
 * </ul>
 */
@Service
public class RecordPipeline {

    private static final Logger log = LoggerFactory.getLogger(RecordPipeline.class);

    public enum Outcome { WRITTEN, REJECTED }

    private final RefDataClient refData;
    private final RecordProcessor processor;
    private final SftrCsvWriter csvWriter;
    private final SftrConstants constants;
    private final ObjectMapper mapper;
    private final Path outputFile;
    private final Path rejectsFile;
    private final boolean flushEachRecord;
    private final String configuredFileDate;

    private SftrCsvWriter.Session csv;
    private BufferedWriter rejects;

    public RecordPipeline(RefDataClient refData, RecordProcessor processor, SftrCsvWriter csvWriter,
                          SftrConstants constants, ObjectMapper mapper,
                          @Value("${sftr.output.file}") String outputFile,
                          @Value("${sftr.output.rejects-file:}") String rejectsFile,
                          @Value("${sftr.output.flush-each-record:true}") boolean flushEachRecord,
                          @Value("${sftr.output.file-date:}") String configuredFileDate) {
        this.refData = refData;
        this.processor = processor;
        this.csvWriter = csvWriter;
        this.constants = constants;
        this.mapper = mapper;
        this.outputFile = Path.of(outputFile);
        this.rejectsFile = (rejectsFile == null || rejectsFile.isBlank())
                ? this.outputFile.resolveSibling(this.outputFile.getFileName() + ".rejects")
                : Path.of(rejectsFile);
        this.flushEachRecord = flushEachRecord;
        this.configuredFileDate = configuredFileDate;
    }

    /**
     * @param json      the message value as received
     * @param sourceRef where it came from (for example topic-partition@offset); goes to the rejects file
     */
    public Outcome handle(String json, String sourceRef) throws IOException {
        SftrTransactionReport report;
        try {
            report = map(json);
        } catch (RefDataClient.UnavailableException e) {
            throw e;                                   // transient: do not skip, the caller retries
        } catch (RuntimeException e) {
            reject(sourceRef, summary(e));
            return Outcome.REJECTED;
        }
        if (csv == null) csv = csvWriter.openAppend(outputFile, processor.columnDefs());
        csv.write(report);
        if (flushEachRecord) csv.flush();
        return Outcome.WRITTEN;
    }

    private SftrTransactionReport map(String json) {
        if (json == null || json.isBlank()) throw new IllegalArgumentException("Empty message");
        JsonNode message = mapper.readTree(json);
        if (message == null || !message.isObject()) throw new IllegalArgumentException("Not a JSON object");
        JsonNode ref = refData.fetch(message);
        String fileDate = (configuredFileDate == null || configuredFileDate.isBlank())
                ? LocalDate.now().toString() : configuredFileDate;
        return processor.process(message, new ProcessingContext(ref, constants, fileDate));
    }

    private void reject(String sourceRef, String summary) throws IOException {
        if (rejects == null) {
            Path parent = rejectsFile.toAbsolutePath().getParent();
            if (parent != null) Files.createDirectories(parent);
            rejects = Files.newBufferedWriter(rejectsFile, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        rejects.write(sourceRef + "\t" + summary + "\n");
        rejects.flush();
        log.warn("Rejected {}: {}", sourceRef, summary);
    }

    /** Error summary for the rejects file. Never includes message content (parser messages quote the input). */
    private static String summary(Throwable t) {
        String text = switch (t) {
            case JacksonException _ -> "Malformed JSON";
            case HttpStatusCodeException h -> "Reference data HTTP " + h.getStatusCode().value();
            case IllegalStateException e when e.getMessage() != null -> e.getMessage();
            case IllegalArgumentException e when e.getMessage() != null
                    && (e.getMessage().startsWith("Empty") || e.getMessage().startsWith("Not a JSON")) -> e.getMessage();
            default -> t.getClass().getSimpleName();
        };
        return text.replaceAll("[\\t\\r\\n]+", " ");
    }

    @PreDestroy
    void close() throws IOException {
        if (csv != null) csv.close();
        if (rejects != null) rejects.close();
    }
}
