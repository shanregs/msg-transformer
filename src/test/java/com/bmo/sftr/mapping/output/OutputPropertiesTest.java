package com.bmo.sftr.mapping.output;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bmo.sftr.mapping.output.OutputProperties.WriteMode;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

class OutputPropertiesTest {

    private static OutputProperties bind(String... keyValues) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("sftr.output.file", "./data/report.csv");
        for (int i = 0; i < keyValues.length; i += 2) map.put(keyValues[i], keyValues[i + 1]);
        return new Binder(new MapConfigurationPropertySource(map)).bind("sftr.output", OutputProperties.class).get();
    }

    @Test
    void defaultsMatchTheDesign() {
        OutputProperties p = bind();
        assertThat(p.file()).isEqualTo("./data/report.csv");
        assertThat(p.writeMode()).isEqualTo(WriteMode.PER_RECORD);
        assertThat(p.batchSize()).isEqualTo(10);
        assertThat(p.batchInterval()).isEqualTo(Duration.ofMinutes(5));
        assertThat(p.maxBufferedRecords()).isEqualTo(1000);
    }

    @Test
    void countModeBindsItsSettings() {
        OutputProperties p = bind("sftr.output.write-mode", "COUNT", "sftr.output.batch-size", "25");
        assertThat(p.writeMode()).isEqualTo(WriteMode.COUNT);
        assertThat(p.batchSize()).isEqualTo(25);
    }

    @Test
    void intervalModeBindsDurations() {
        OutputProperties p = bind("sftr.output.write-mode", "INTERVAL", "sftr.output.batch-interval", "90s");
        assertThat(p.writeMode()).isEqualTo(WriteMode.INTERVAL);
        assertThat(p.batchInterval()).isEqualTo(Duration.ofSeconds(90));
    }

    @Test
    void rejectsFileAndFileDateAreOptional() {
        OutputProperties p = bind("sftr.output.file-date", "2026-01-01", "sftr.output.rejects-file", "./r.txt");
        assertThat(p.fileDate()).isEqualTo("2026-01-01");
        assertThat(p.rejectsFile()).isEqualTo("./r.txt");
    }

    @Test
    void fileIsRequired() {
        assertThatThrownBy(() -> new OutputProperties(" ", null, null, WriteMode.PER_RECORD, 10, Duration.ofMinutes(5), 1000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("sftr.output.file is required");
    }

    @Test
    void countModeNeedsAPositiveBatchSize() {
        assertThatThrownBy(() -> bind("sftr.output.write-mode", "COUNT", "sftr.output.batch-size", "0"))
                .rootCause().hasMessage("sftr.output.batch-size must be at least 1 when write-mode is COUNT");
    }

    @Test
    void intervalModeNeedsAPositiveInterval() {
        assertThatThrownBy(() -> bind("sftr.output.write-mode", "INTERVAL", "sftr.output.batch-interval", "0s"))
                .rootCause().hasMessage("sftr.output.batch-interval must be greater than zero when write-mode is INTERVAL");
        assertThatThrownBy(() -> bind("sftr.output.write-mode", "INTERVAL", "sftr.output.batch-interval", "-1m"))
                .rootCause().hasMessageContaining("sftr.output.batch-interval");
    }

    @Test
    void unusedModeSettingsAreNotChecked() {
        assertThat(bind("sftr.output.write-mode", "PER_RECORD", "sftr.output.batch-size", "0").writeMode())
                .isEqualTo(WriteMode.PER_RECORD);
        assertThat(bind("sftr.output.write-mode", "INTERVAL", "sftr.output.batch-size", "0").writeMode())
                .isEqualTo(WriteMode.INTERVAL);
    }

    @Test
    void bufferCapMustBePositiveAndNotBelowTheBatchSize() {
        assertThatThrownBy(() -> bind("sftr.output.max-buffered-records", "0"))
                .rootCause().hasMessage("sftr.output.max-buffered-records must be at least 1");
        assertThatThrownBy(() -> bind("sftr.output.write-mode", "COUNT", "sftr.output.batch-size", "50",
                "sftr.output.max-buffered-records", "10"))
                .rootCause().hasMessage("sftr.output.max-buffered-records must not be smaller than batch-size");
    }

    @Test
    void unknownWriteModeFailsTheBinding() {
        assertThatThrownBy(() -> bind("sftr.output.write-mode", "WEEKLY")).isInstanceOf(RuntimeException.class);
    }

    @Test
    void fileDateMustBeAnIsoDate() {
        assertThatThrownBy(() -> bind("sftr.output.file-date", "31/01/2026"))
                .rootCause().hasMessage("sftr.output.file-date must be an ISO date such as 2026-01-31");
    }
}
