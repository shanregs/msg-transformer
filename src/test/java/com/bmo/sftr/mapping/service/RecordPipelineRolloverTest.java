package com.bmo.sftr.mapping.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bmo.sftr.mapping.TestSupport;
import com.bmo.sftr.mapping.engine.MappingConfigLoaderTest;
import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.output.OutputProperties;
import com.bmo.sftr.mapping.output.OutputProperties.Rollover;
import com.bmo.sftr.mapping.output.OutputProperties.WriteMode;
import com.bmo.sftr.mapping.output.SftrCsvWriter;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.JsonNode;

/** The pipeline crossing midnight in Asia/Kolkata with daily files. */
class RecordPipelineRolloverTest {

    private static final ZoneId KOLKATA = ZoneId.of("Asia/Kolkata");
    private static final JsonNode REF = TestSupport.MAPPER.readTree("""
            {"find_sftr_party":{"values":{"lei":"549300ABCDEFGHIJ0001"}},
             "find_sftr_party_sector":{"values":{"natureOfReportingCounterparty":"C","sector":"CDTI"}}}""");

    /** A clock the test can move forward. */
    static final class MovableClock extends Clock {
        private Instant now;

        MovableClock(String isoInstant) {
            now = Instant.parse(isoInstant);
        }

        void set(String isoInstant) {
            now = Instant.parse(isoInstant);
        }

        @Override public ZoneId getZone() { return KOLKATA; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    @TempDir
    Path dir;

    private static String msg(String ref) {
        return "{\"trade_ref\":\"" + ref + "\",\"trade_type\":\"PO\",\"bmoLegalEntity\":\"LDNBB01\"}";
    }

    @Test
    void rowsMoveToANewFileAtMidnightInKolkata() throws Exception {
        RefDataClient refData = mock(RefDataClient.class);
        when(refData.fetch(any())).thenReturn(REF);
        RecordProcessor processor = new RecordProcessor(MappingConfigLoaderTest.loader("mapper/sftr-field-mappings.json"));
        OutputProperties props = new OutputProperties(dir.resolve("report.csv").toString(), null,
                Rollover.DAILY, KOLKATA, WriteMode.PER_RECORD, 10, Duration.ofMinutes(5), 1000);
        // 18:29 UTC is 23:59 on 16 Oct in Kolkata; 18:31 UTC is 00:01 on 17 Oct
        MovableClock clock = new MovableClock("2026-10-16T18:29:00Z");
        RecordPipeline p = new RecordPipeline(refData, processor, TestSupport.csvWriter(),
                TestSupport.realConstants(), TestSupport.MAPPER, props, clock);

        p.handle(msg("1"), "t:1");
        assertThatThrownBy(() -> p.handle("{not json", "t:2")).isInstanceOf(RecordProcessingException.class);
        clock.set("2026-10-16T18:31:00Z");
        p.handle(msg("2"), "t:3");
        p.close();

        Path day1 = dir.resolve("report-2026-10-16.csv");
        Path day2 = dir.resolve("report-2026-10-17.csv");
        assertThat(Files.readAllLines(day1)).hasSize(2);
        assertThat(Files.readAllLines(day2)).hasSize(2);
        assertThat(Files.readAllLines(day2).get(0)).startsWith("IM_SubmitterID,");
        assertThat(dir.resolve("report.csv")).doesNotExist();
        assertThat(dir.resolve("report-2026-10-16.csv.rejects")).doesNotExist();
    }
}
