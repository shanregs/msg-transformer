package com.bmo.sftr.mapping.output;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Settings under {@code sftr.output}. Invalid values fail the start with a message naming the key.
 *
 * @param file               the CSV report; rows are appended
 * @param rejectsFile        rejected messages; blank means {@code <file>.rejects}
 * @param fileDate           ISO date written into the report; blank means today
 * @param flushEachRecord    flush after every row (used by the sink that keeps the file open)
 * @param writeMode          when rows are written, see {@link WriteMode}
 * @param batchSize          rows per batch, used by {@link WriteMode#COUNT}
 * @param batchInterval      maximum age of the oldest buffered row, used by {@link WriteMode#INTERVAL}
 * @param maxBufferedRecords safety cap on rows held in memory; reaching it forces a write
 */
@ConfigurationProperties(prefix = "sftr.output")
public record OutputProperties(
        String file,
        String rejectsFile,
        String fileDate,
        @DefaultValue("true") boolean flushEachRecord,
        @DefaultValue("PER_RECORD") WriteMode writeMode,
        @DefaultValue("10") int batchSize,
        @DefaultValue("5m") Duration batchInterval,
        @DefaultValue("1000") int maxBufferedRecords) {

    /** When rows reach the CSV file. */
    public enum WriteMode {
        /** Open the file, append the row, close the file, for every message. */
        PER_RECORD,
        /** Buffer rows and write them as one batch once the oldest row is {@code batchInterval} old. */
        INTERVAL,
        /** Buffer rows and write them as one batch once {@code batchSize} rows are waiting. */
        COUNT
    }

    public OutputProperties {
        if (file == null || file.isBlank()) throw invalid("sftr.output.file", "is required");
        if (writeMode == null) writeMode = WriteMode.PER_RECORD;
        if (batchInterval == null) batchInterval = Duration.ofMinutes(5);
        if (fileDate != null && !fileDate.isBlank()) {
            try {
                LocalDate.parse(fileDate.trim());
            } catch (DateTimeException e) {
                throw invalid("sftr.output.file-date", "must be an ISO date such as 2026-01-31");
            }
        }
        if (maxBufferedRecords < 1) throw invalid("sftr.output.max-buffered-records", "must be at least 1");
        switch (writeMode) {
            case PER_RECORD -> { }
            case COUNT -> {
                if (batchSize < 1) throw invalid("sftr.output.batch-size", "must be at least 1 when write-mode is COUNT");
                if (maxBufferedRecords < batchSize) {
                    throw invalid("sftr.output.max-buffered-records", "must not be smaller than batch-size");
                }
            }
            case INTERVAL -> {
                if (batchInterval.isZero() || batchInterval.isNegative()) {
                    throw invalid("sftr.output.batch-interval", "must be greater than zero when write-mode is INTERVAL");
                }
            }
        }
    }

    private static IllegalArgumentException invalid(String key, String problem) {
        return new IllegalArgumentException(key + " " + problem);
    }
}
