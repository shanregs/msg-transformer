package com.bmo.sftr.mapping.input;

import com.bmo.sftr.mapping.service.RecordPipeline;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Batch alternative to the Kafka listener: reads messages from a file, one after another, and sends
 * each through {@link RecordPipeline}, which appends the rows to the output CSV.
 *
 * <ul>
 *   <li>{@code .csv}: first line is the header; each following line becomes a flat JSON object
 *       (column name to text value) before mapping.</li>
 *   <li>anything else (for example {@code .txt}): one JSON message per line; blank lines are skipped.</li>
 * </ul>
 *
 * A bad line is logged and skipped; the run carries on with the next one. Enabled by setting
 * {@code sftr.input.file}; run with the {@code file} profile so the Kafka listener is off.
 */
@Component
@ConditionalOnProperty(name = "sftr.input.file")
public class FileInputRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(FileInputRunner.class);

    private final RecordPipeline pipeline;
    private final ObjectMapper mapper;
    private final Path input;
    private final char delimiter;

    public FileInputRunner(RecordPipeline pipeline, ObjectMapper mapper,
                           @Value("${sftr.input.file}") String input,
                           @Value("${sftr.input.delimiter:,}") char delimiter) {
        this.pipeline = pipeline;
        this.mapper = mapper;
        this.input = Path.of(input);
        this.delimiter = delimiter;
    }

    @Override
    public void run(ApplicationArguments args) throws IOException {
        int[] counts = new int[2];                                  // written, rejected-or-failed
        if (input.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".csv")) {
            readCsv(counts);
        } else {
            readLines(counts);
        }
        log.info("Finished {}: {} written, {} rejected or failed", input, counts[0], counts[1]);
    }

    private void readLines(int[] counts) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8)) {
            String line;
            int n = 0;
            while ((line = reader.readLine()) != null) {
                n++;
                if (line.isBlank()) continue;
                handle(line, input.getFileName() + ":" + n, counts);
            }
        }
    }

    private void readCsv(int[] counts) throws IOException {
        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter).setHeader().setSkipHeaderRecord(true)
                .setIgnoreEmptyLines(true).setTrim(true).get();
        try (BufferedReader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8);
             CSVParser parser = format.parse(reader)) {
            for (CSVRecord rec : parser) {
                String ref = input.getFileName() + ":" + (rec.getRecordNumber() + 1);
                String json;
                try {
                    ObjectNode node = mapper.createObjectNode();
                    rec.toMap().forEach(node::put);
                    json = mapper.writeValueAsString(node);
                } catch (RuntimeException e) {
                    log.error("Failed to read {}; skipping to the next line", ref, e);
                    counts[1]++;
                    continue;
                }
                handle(json, ref, counts);
            }
        }
    }

    private void handle(String json, String ref, int[] counts) {
        try {
            if (pipeline.handle(json, ref) == RecordPipeline.Outcome.WRITTEN) counts[0]++; else counts[1]++;
        } catch (Exception e) {
            log.error("Failed to process {}; skipping to the next line", ref, e);
            counts[1]++;
        }
    }
}
