package com.bmo.sftr.mapping.service;

import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.model.ProcessingContext;
import com.bmo.sftr.mapping.model.SftrConstants;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import com.bmo.sftr.mapping.output.SftrCsvWriter;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpStatusCodeException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Service
public class SftrReportService {

    private static final Logger log = LoggerFactory.getLogger(SftrReportService.class);

    public record RunResult(long read, long written, long rejected) {}

    private record Pending(long lineNo, Future<SftrTransactionReport> future) {}

    private final RefDataClient refData;
    private final RecordProcessor processor;
    private final SftrCsvWriter csvWriter;
    private final SftrConstants constants;
    private final ObjectMapper mapper;
    private final int windowSize;
    private final Semaphore apiPermits;

    public SftrReportService(RefDataClient refData, RecordProcessor processor, SftrCsvWriter csvWriter,
                             SftrConstants constants, ObjectMapper mapper,
                             @Value("${sftr.processing.window-size:1000}") int windowSize,
                             @Value("${sftr.processing.max-concurrent-ref-data-calls:20}") int maxConcurrentCalls) {
        if (windowSize < 1) throw new IllegalArgumentException("sftr.processing.window-size must be >= 1");
        if (maxConcurrentCalls < 1) {
            throw new IllegalArgumentException("sftr.processing.max-concurrent-ref-data-calls must be >= 1");
        }
        this.refData = refData;
        this.processor = processor;
        this.csvWriter = csvWriter;
        this.constants = constants;
        this.mapper = mapper;
        this.windowSize = windowSize;
        this.apiPermits = new Semaphore(maxConcurrentCalls);
    }

    public RunResult run(Path input, Path output, String fileDate) throws IOException {
        Path rejectsFile = output.resolveSibling(output.getFileName() + ".rejects");
        long read = 0;
        long written = 0;
        long rejected = 0;

        try (BufferedReader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8);
             SftrCsvWriter.Session csv = csvWriter.open(output, processor.columnDefs());
             BufferedWriter rejects = Files.newBufferedWriter(rejectsFile, StandardCharsets.UTF_8);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            List<Pending> window = new ArrayList<>(windowSize);
            long lineNo = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                if (line.isBlank()) continue;
                read++;
                final String json = line;
                window.add(new Pending(lineNo, executor.submit(() -> mapLine(json, fileDate))));
                if (window.size() >= windowSize) {
                    long[] counts = drain(window, csv, rejects);
                    written += counts[0];
                    rejected += counts[1];
                }
            }
            long[] counts = drain(window, csv, rejects);
            written += counts[0];
            rejected += counts[1];
        }

        if (rejected == 0) Files.deleteIfExists(rejectsFile);
        log.info("SFTR run finished: read={}, written={}, rejected={}", read, written, rejected);
        return new RunResult(read, written, rejected);
    }

    /** Takes the window's results in submission order. Returns {written, rejected}. */
    private long[] drain(List<Pending> window, SftrCsvWriter.Session csv, BufferedWriter rejects) throws IOException {
        long written = 0;
        long rejected = 0;
        for (Pending p : window) {
            try {
                csv.write(p.future().get());
                written++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for line " + p.lineNo(), e);
            } catch (ExecutionException e) {
                rejects.write(p.lineNo() + "\t" + summary(e.getCause()) + "\n");
                rejected++;
                log.warn("Rejected line {}: {}", p.lineNo(), summary(e.getCause()));
            }
        }
        window.clear();
        return new long[] {written, rejected};
    }

    private SftrTransactionReport mapLine(String line, String fileDate) throws InterruptedException {
        JsonNode message = mapper.readTree(line);
        if (message == null || !message.isObject()) throw new IllegalArgumentException("Not a JSON object");
        JsonNode ref = fetchLimited(message);
        return processor.process(message, new ProcessingContext(ref, constants, fileDate));
    }

    private JsonNode fetchLimited(JsonNode message) throws InterruptedException {
        apiPermits.acquire();
        try {
            return refData.fetch(message);
        } finally {
            apiPermits.release();
        }
    }

    /** Error summary for the rejects file. Never includes input content (parser messages quote the input). */
    private static String summary(Throwable t) {
        String text = switch (t) {
            case tools.jackson.core.JacksonException _ -> "Malformed JSON";
            case HttpStatusCodeException h -> "Reference data HTTP " + h.getStatusCode().value();
            case IllegalStateException e when e.getMessage() != null -> e.getMessage();
            case null -> "Unknown error";
            default -> t.getClass().getSimpleName();
        };
        return text.replaceAll("[\\t\\r\\n]+", " ");
    }
}
