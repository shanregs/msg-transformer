package com.bmo.sftr.mapping.output;

import static org.assertj.core.api.Assertions.assertThat;

import com.bmo.sftr.mapping.output.OutputProperties.Rollover;
import com.bmo.sftr.mapping.output.OutputProperties.WriteMode;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class OutputFileResolverTest {

    private static final LocalDate DAY = LocalDate.of(2026, 10, 16);
    private static final ZoneId KOLKATA = ZoneId.of("Asia/Kolkata");

    private static OutputProperties props(String file, String rejects, String fileDate, Rollover rollover) {
        return new OutputProperties(file, rejects, fileDate, rollover, KOLKATA, WriteMode.PER_RECORD, 10,
                Duration.ofMinutes(5), 1000);
    }

    @Test
    void dailyAddsTheDateBeforeTheExtension() {
        OutputFileResolver r = new OutputFileResolver(props("./data/sftr-report.csv", null, null, Rollover.DAILY));
        assertThat(r.report(DAY)).isEqualTo(Path.of("./data/sftr-report-2026-10-16.csv"));
    }

    @Test
    void dailyAddsTheDateAtTheEndWhenThereIsNoExtension() {
        OutputFileResolver r = new OutputFileResolver(props("data/report", null, null, Rollover.DAILY));
        assertThat(r.report(DAY)).isEqualTo(Path.of("data/report-2026-10-16"));
    }

    @Test
    void rejectsDefaultToTheDaysReportFileWithASuffix() {
        OutputFileResolver r = new OutputFileResolver(props("./data/sftr-report.csv", null, null, Rollover.DAILY));
        assertThat(r.rejects(DAY)).isEqualTo(Path.of("./data/sftr-report-2026-10-16.csv.rejects"));
    }

    @Test
    void aConfiguredRejectsFileAlsoRollsDaily() {
        OutputFileResolver r = new OutputFileResolver(props("./data/r.csv", "./data/rejects.txt", null, Rollover.DAILY));
        assertThat(r.rejects(DAY)).isEqualTo(Path.of("./data/rejects-2026-10-16.txt"));
    }

    @Test
    void noRolloverKeepsTheConfiguredNames() {
        OutputFileResolver r = new OutputFileResolver(props("./data/r.csv", null, null, Rollover.NONE));
        assertThat(r.report(DAY)).isEqualTo(Path.of("./data/r.csv"));
        assertThat(r.rejects(DAY)).isEqualTo(Path.of("./data/r.csv.rejects"));
        OutputFileResolver custom = new OutputFileResolver(props("./data/r.csv", "./data/rej.txt", null, Rollover.NONE));
        assertThat(custom.rejects(DAY)).isEqualTo(Path.of("./data/rej.txt"));
    }

    @Test
    void theDayFollowsMidnightInTheConfiguredZoneNotUtc() {
        // 2026-10-16 19:00 UTC is 2026-10-17 00:30 in Kolkata
        Clock clock = Clock.fixed(Instant.parse("2026-10-16T19:00:00Z"), KOLKATA);
        ReportDay day = new ReportDay(props("r.csv", null, null, Rollover.DAILY), clock);
        assertThat(day.today()).isEqualTo(LocalDate.of(2026, 10, 17));
    }

    @Test
    void aConfiguredFileDateReplacesTodayInRowsAndFileNames() {
        Clock clock = Clock.fixed(Instant.parse("2026-10-16T19:00:00Z"), KOLKATA);
        OutputProperties p = props("data/r.csv", null, "2026-01-01", Rollover.DAILY);
        LocalDate day = new ReportDay(p, clock).today();
        assertThat(day).isEqualTo(LocalDate.of(2026, 1, 1));
        assertThat(new OutputFileResolver(p).report(day)).isEqualTo(Path.of("data/r-2026-01-01.csv"));
    }
}
