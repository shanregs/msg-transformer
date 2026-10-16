package com.bmo.sftr.mapping.output;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Settings under {@code sftr.output}. Invalid values fail the start with a message naming the key.
 *
 * @param file               the CSV report; with {@code rollover: DAILY} the date is added before the extension
 * @param rejectsFile        rejected messages; blank means {@code <report file>.rejects}
 * @param fileDate           ISO date that replaces today's date in the rows and the file names; blank means today
 * @param rollover           whether a new file is started each day, see {@link Rollover}
 * @param fileZone           the time zone whose midnight starts a new day
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
        @DefaultValue("DAILY") Rollover rollover,
        @DefaultValue("Asia/Kolkata") ZoneId fileZone,
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

    /** Whether the report and rejects files are split by day. */
    public enum Rollover {
        /** One file per day: {@code sftr-report-2026-10-16.csv}. A file is created when its first row arrives. */
        DAILY,
        /** One file with the configured name. */
        NONE
    }

    public OutputProperties {
        if (file == null || file.isBlank()) throw invalid("sftr.output.file", "is required");
        if (rollover == null) rollover = Rollover.DAILY;
        if (fileZone == null) fileZone = ZoneId.of("Asia/Kolkata");
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

    /** The pinned date, or null when today's date should be used. */
    public LocalDate pinnedDate() {
        return fileDate == null || fileDate.isBlank() ? null : LocalDate.parse(fileDate.trim());
    }

    private static IllegalArgumentException invalid(String key, String problem) {
        return new IllegalArgumentException(key + " " + problem);
    }
}
