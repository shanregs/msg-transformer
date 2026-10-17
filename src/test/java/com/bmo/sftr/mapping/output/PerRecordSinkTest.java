package com.bmo.sftr.mapping.output;

import com.bmo.sftr.mapping.TestSupport;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bmo.sftr.mapping.engine.MappingConfigLoaderTest;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import com.bmo.sftr.mapping.output.OutputProperties.Rollover;
import com.bmo.sftr.mapping.output.OutputProperties.WriteMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PerRecordSinkTest {

    static final LocalDate DAY = LocalDate.of(2026, 10, 16);

    @TempDir
    Path dir;

    static OutputProperties props(Path file, Rollover rollover, WriteMode mode) {
        return new OutputProperties(file.toString(), null, rollover, ZoneId.of("Asia/Kolkata"), mode, 10,
                Duration.ofMinutes(5), 1000);
    }

    private PerRecordSink sink(SftrCsvWriter writer, Path out, Rollover rollover) {
        return new PerRecordSink(writer, new OutputFileResolver(props(out, rollover, WriteMode.PER_RECORD)));
    }

    private static SftrTransactionReport row(String side) {
        SftrTransactionReport r = new SftrTransactionReport();
        r.setCpCpSide(side);
        return r;
    }

    private List<String> sides(Path out) throws Exception {
        return Files.readAllLines(out).stream().skip(1).map(l -> l.split(",", -1)[12]).toList();
    }

    @Test
    void opensAndClosesTheFileForEveryRow() throws Exception {
        Path out = dir.resolve("out.csv");
        SftrCsvWriter writer = spy(TestSupport.csvWriter());
        PerRecordSink sink = sink(writer, out, Rollover.NONE);

        sink.append(row("GIVE"), DAY);
        sink.append(row("TAKE"), DAY);
        sink.append(row("GIVE"), DAY);

        verify(writer, times(3)).openAppend(any());
        assertThat(sides(out)).containsExactly("GIVE", "TAKE", "GIVE");
    }

    @Test
    void headerIsWrittenOnceAndTheFileIsNotLeftOpen() throws Exception {
        Path out = dir.resolve("out.csv");
        PerRecordSink sink = sink(TestSupport.csvWriter(), out, Rollover.NONE);
        sink.append(row("GIVE"), DAY);
        sink.append(row("TAKE"), DAY);

        List<String> lines = Files.readAllLines(out);
        assertThat(lines).hasSize(3);
        assertThat(lines.stream().filter(l -> l.startsWith("IM_SubmitterID,")).count()).isEqualTo(1);
        Path moved = dir.resolve("moved.csv");
        Files.move(out, moved);                                   // fails on Windows if a handle is still open
        assertThat(moved).exists();
    }

    @Test
    void appendsToAnExistingFileWithoutASecondHeader() throws Exception {
        Path out = dir.resolve("out.csv");
        sink(TestSupport.csvWriter(), out, Rollover.NONE).append(row("GIVE"), DAY);
        sink(TestSupport.csvWriter(), out, Rollover.NONE).append(row("TAKE"), DAY);
        assertThat(Files.readAllLines(out)).hasSize(3);
        assertThat(sides(out)).containsExactly("GIVE", "TAKE");
    }

    @Test
    void dailyRolloverStartsANewFileWithItsOwnHeaderForEachDay() throws Exception {
        Path base = dir.resolve("report.csv");
        PerRecordSink sink = sink(TestSupport.csvWriter(), base, Rollover.DAILY);
        sink.append(row("GIVE"), DAY);
        sink.append(row("TAKE"), DAY);
        sink.append(row("GIVE"), DAY.plusDays(1));

        Path day1 = dir.resolve("report-2026-10-16.csv");
        Path day2 = dir.resolve("report-2026-10-17.csv");
        assertThat(sides(day1)).containsExactly("GIVE", "TAKE");
        assertThat(sides(day2)).containsExactly("GIVE");
        assertThat(Files.readAllLines(day2).get(0)).startsWith("IM_SubmitterID,");
        assertThat(base).doesNotExist();
    }

    @Test
    void aRestartOnTheSameDayAppendsToThatDaysFile() throws Exception {
        Path base = dir.resolve("report.csv");
        sink(TestSupport.csvWriter(), base, Rollover.DAILY).append(row("GIVE"), DAY);
        sink(TestSupport.csvWriter(), base, Rollover.DAILY).append(row("TAKE"), DAY);
        assertThat(sides(dir.resolve("report-2026-10-16.csv"))).containsExactly("GIVE", "TAKE");
    }

    @Test
    void flushAndCloseAreHarmless() throws Exception {
        PerRecordSink sink = sink(TestSupport.csvWriter(), dir.resolve("out.csv"), Rollover.NONE);
        sink.flush();
        sink.close();
        sink.append(row("GIVE"), DAY);
        sink.flush();
        sink.close();
        assertThat(sides(dir.resolve("out.csv"))).containsExactly("GIVE");
    }

    @Test
    void callbackRunsAfterTheRowIsWritten() throws Exception {
        Path out = dir.resolve("out.csv");
        PerRecordSink sink = sink(TestSupport.csvWriter(), out, Rollover.NONE);
        List<Integer> linesWhenCalled = new java.util.ArrayList<>();
        sink.append(row("GIVE"), DAY, () -> {
            try {
                linesWhenCalled.add(Files.readAllLines(out).size());
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        });
        assertThat(linesWhenCalled).containsExactly(2);          // header + the row, already on disk
        List<String> later = new java.util.ArrayList<>();
        sink.whenDurable(() -> later.add("ran"));
        assertThat(later).containsExactly("ran");
    }

    @Test
    void perRecordModeSelectsThisSink() {
        OutputProperties props = props(dir.resolve("o.csv"), Rollover.DAILY, WriteMode.PER_RECORD);
        assertThat(new ReportSinkConfig().reportSink(props, TestSupport.csvWriter(), new OutputFileResolver(props), java.time.Clock.systemUTC()))
                .isInstanceOf(PerRecordSink.class);
    }
}
