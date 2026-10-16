package com.bmo.sftr.mapping.output;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bmo.sftr.mapping.engine.MappingConfigLoaderTest;
import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import com.bmo.sftr.mapping.output.OutputProperties.Rollover;
import com.bmo.sftr.mapping.output.OutputProperties.WriteMode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BatchingSinkTest {

    static final ZoneId KOLKATA = ZoneId.of("Asia/Kolkata");
    static final LocalDate DAY1 = LocalDate.of(2026, 10, 16);
    static final LocalDate DAY2 = DAY1.plusDays(1);

    /** A clock the test moves by hand. */
    static final class MovableClock extends Clock {
        private Instant now;

        MovableClock(String iso) {
            now = Instant.parse(iso);
        }

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override public ZoneId getZone() { return KOLKATA; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    /** Fails the first {@code failures} attempts to open the file, then behaves normally. */
    static final class FlakyWriter extends SftrCsvWriter {
        int failures;

        FlakyWriter(int failures) {
            super(',');
            this.failures = failures;
        }

        @Override
        public Session openAppend(Path file, List<com.bmo.sftr.mapping.engine.MappingConfigLoader.Column> columns) throws IOException {
            if (failures-- > 0) throw new IOException("disk full");
            return super.openAppend(file, columns);
        }
    }

    @TempDir
    Path dir;

    // 10:00 in Kolkata on 16 Oct
    private final MovableClock clock = new MovableClock("2026-10-16T04:30:00Z");
    private final RecordProcessor processor = new RecordProcessor(MappingConfigLoaderTest.loader("sftr-field-mappings.json"));

    private BatchingSink sink(WriteMode mode, int size, Duration interval, int max, SftrCsvWriter writer) {
        OutputProperties props = new OutputProperties(dir.resolve("r.csv").toString(), null, null, Rollover.DAILY, KOLKATA,
                mode, size, interval, max);
        return new BatchingSink(writer, processor, new OutputFileResolver(props), props, clock, false);
    }

    private static SftrTransactionReport row(String side) {
        SftrTransactionReport r = new SftrTransactionReport();
        r.setCpCpSide(side);
        return r;
    }

    private Path day(LocalDate d) {
        return dir.resolve("r-" + d + ".csv");
    }

    private List<String> sides(Path out) throws Exception {
        return Files.readAllLines(out).stream().skip(1).map(l -> l.split(",", -1)[12]).toList();
    }

    @Test
    void countModeWritesOnlyWhenTheBatchIsFullAndKeepsOrder() throws Exception {
        BatchingSink s = sink(WriteMode.COUNT, 3, Duration.ofMinutes(5), 1000, new SftrCsvWriter(','));
        s.append(row("GIVE"), DAY1);
        s.append(row("TAKE"), DAY1);
        assertThat(day(DAY1)).doesNotExist();
        assertThat(s.buffered()).isEqualTo(2);

        s.append(row("GIVE"), DAY1);                       // third row completes the batch
        assertThat(sides(day(DAY1))).containsExactly("GIVE", "TAKE", "GIVE");
        assertThat(s.buffered()).isZero();

        s.append(row("TAKE"), DAY1);
        s.append(row("TAKE"), DAY1);
        s.append(row("GIVE"), DAY1);                       // second batch appended, one header only
        assertThat(sides(day(DAY1))).containsExactly("GIVE", "TAKE", "GIVE", "TAKE", "TAKE", "GIVE");
        assertThat(Files.readAllLines(day(DAY1)).stream().filter(l -> l.startsWith("IM_SubmitterID,")).count()).isEqualTo(1);
    }

    @Test
    void intervalModeWaitsForTheOldestRowToReachTheInterval() throws Exception {
        BatchingSink s = sink(WriteMode.INTERVAL, 10, Duration.ofMinutes(5), 1000, new SftrCsvWriter(','));
        s.append(row("GIVE"), DAY1);
        clock.advance(Duration.ofMinutes(4));
        s.append(row("TAKE"), DAY1);                       // newer row does not restart the timer
        assertThat(s.flushIfDue()).isFalse();
        assertThat(day(DAY1)).doesNotExist();

        clock.advance(Duration.ofMinutes(1));              // oldest row is now 5 minutes old
        assertThat(s.flushIfDue()).isTrue();
        assertThat(sides(day(DAY1))).containsExactly("GIVE", "TAKE");
        assertThat(s.buffered()).isZero();
        assertThat(s.flushIfDue()).isFalse();              // nothing left
    }

    @Test
    void intervalModeIgnoresTheCountButStillHonoursTheBufferCap() throws Exception {
        BatchingSink s = sink(WriteMode.INTERVAL, 1, Duration.ofMinutes(5), 2, new SftrCsvWriter(','));
        s.append(row("GIVE"), DAY1);
        s.append(row("TAKE"), DAY1);
        assertThat(day(DAY1)).doesNotExist();              // batch-size is not used in INTERVAL mode
        s.append(row("GIVE"), DAY1);                       // cap of 2 reached: the first two are written
        assertThat(sides(day(DAY1))).containsExactly("GIVE", "TAKE");
        assertThat(s.buffered()).isEqualTo(1);
    }

    @Test
    void aNewDayFlushesTheOldDaysRowsFirst() throws Exception {
        BatchingSink s = sink(WriteMode.COUNT, 100, Duration.ofMinutes(5), 1000, new SftrCsvWriter(','));
        s.append(row("GIVE"), DAY1);
        s.append(row("TAKE"), DAY1);
        s.append(row("GIVE"), DAY2);

        assertThat(sides(day(DAY1))).containsExactly("GIVE", "TAKE");
        assertThat(day(DAY2)).doesNotExist();              // the new day's row is still waiting
        s.flush();
        assertThat(sides(day(DAY2))).containsExactly("GIVE");
        assertThat(Files.readAllLines(day(DAY2)).get(0)).startsWith("IM_SubmitterID,");
    }

    @Test
    void rowsFromLateEveningAreWrittenAtMidnightEvenInCountMode() throws Exception {
        BatchingSink s = sink(WriteMode.COUNT, 100, Duration.ofMinutes(5), 1000, new SftrCsvWriter(','));
        clock.advance(Duration.ofHours(13).plusMinutes(58));          // 23:58 in Kolkata
        s.append(row("GIVE"), DAY1);
        clock.advance(Duration.ofMinutes(1));                         // 23:59
        assertThat(s.flushIfDue()).isFalse();
        clock.advance(Duration.ofMinutes(1));                         // 00:00 on 17 Oct
        assertThat(s.flushIfDue()).isTrue();
        assertThat(sides(day(DAY1))).containsExactly("GIVE");
    }

    @Test
    void midnightCutsTheIntervalShort() throws Exception {
        BatchingSink s = sink(WriteMode.INTERVAL, 10, Duration.ofMinutes(30), 1000, new SftrCsvWriter(','));
        clock.advance(Duration.ofHours(13).plusMinutes(58));          // 23:58
        s.append(row("GIVE"), DAY1);
        clock.advance(Duration.ofMinutes(2));                         // 00:00, only 2 of 30 minutes passed
        assertThat(s.flushIfDue()).isTrue();
        assertThat(sides(day(DAY1))).containsExactly("GIVE");
    }

    @Test
    void failedWriteKeepsTheRowsAndRetriesAfterTheDelay() throws Exception {
        BatchingSink s = sink(WriteMode.INTERVAL, 10, Duration.ofMinutes(5), 1000, new FlakyWriter(1));
        s.append(row("GIVE"), DAY1);
        s.append(row("TAKE"), DAY1);
        clock.advance(Duration.ofMinutes(5));

        assertThat(s.flushIfDue()).isFalse();              // disk error: rows stay
        assertThat(s.buffered()).isEqualTo(2);
        assertThat(s.flushIfDue()).isFalse();              // not retried before the delay
        clock.advance(BatchingSink.RETRY_DELAY);
        assertThat(s.flushIfDue()).isTrue();
        assertThat(sides(day(DAY1))).containsExactly("GIVE", "TAKE");     // each row exactly once
    }

    @Test
    void aFailedCountFlushThrowsButNoRowIsLost() throws Exception {
        BatchingSink s = sink(WriteMode.COUNT, 2, Duration.ofMinutes(5), 1000, new FlakyWriter(1));
        s.append(row("GIVE"), DAY1);
        assertThatThrownBy(() -> s.append(row("TAKE"), DAY1)).isInstanceOf(IOException.class);
        assertThat(s.buffered()).isEqualTo(2);
        s.flush();
        assertThat(sides(day(DAY1))).containsExactly("GIVE", "TAKE");
    }

    @Test
    void aFullBufferThatCannotBeWrittenRefusesTheNewRow() throws Exception {
        BatchingSink s = sink(WriteMode.INTERVAL, 10, Duration.ofMinutes(5), 2, new FlakyWriter(1));
        s.append(row("GIVE"), DAY1);
        s.append(row("TAKE"), DAY1);
        assertThatThrownBy(() -> s.append(row("GIVE"), DAY1)).isInstanceOf(IOException.class);
        assertThat(s.buffered()).isEqualTo(2);             // the refused row was not added
        s.append(row("GIVE"), DAY1);                       // disk is back: the first two are written, the new one waits
        assertThat(sides(day(DAY1))).containsExactly("GIVE", "TAKE");
        assertThat(s.buffered()).isEqualTo(1);
    }

    @Test
    void closeWritesWhatIsLeftAndThenRefusesRows() throws Exception {
        BatchingSink s = sink(WriteMode.COUNT, 100, Duration.ofMinutes(5), 1000, new SftrCsvWriter(','));
        s.append(row("GIVE"), DAY1);
        s.close();
        assertThat(sides(day(DAY1))).containsExactly("GIVE");
        assertThatThrownBy(() -> s.append(row("TAKE"), DAY1)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void theFlusherThreadWritesWithoutAnyFurtherMessages() throws Exception {
        OutputProperties props = new OutputProperties(dir.resolve("r.csv").toString(), null, null, Rollover.DAILY, KOLKATA,
                WriteMode.INTERVAL, 10, Duration.ofMillis(200), 1000);
        Clock real = Clock.system(KOLKATA);
        BatchingSink s = new BatchingSink(new SftrCsvWriter(','), processor, new OutputFileResolver(props), props, real);
        LocalDate today = LocalDate.now(real);
        try {
            s.append(row("GIVE"), today);
            s.append(row("TAKE"), today);
            Path out = dir.resolve("r-" + today + ".csv");
            long end = System.currentTimeMillis() + 10_000;
            while (!Files.exists(out) && System.currentTimeMillis() < end) Thread.sleep(50);
            assertThat(out).exists();
            assertThat(sides(out)).containsExactly("GIVE", "TAKE");
        } finally {
            s.close();
        }
    }

    @Test
    void callbacksRunInOrderOnlyAfterTheirRowsAreWritten() throws Exception {
        BatchingSink s = sink(WriteMode.COUNT, 2, Duration.ofMinutes(5), 1000, new SftrCsvWriter(','));
        List<String> done = new java.util.ArrayList<>();
        s.append(row("GIVE"), DAY1, () -> done.add("first"));
        s.whenDurable(() -> done.add("reject-after-first"));
        assertThat(done).isEmpty();                                  // nothing is on disk yet

        s.append(row("TAKE"), DAY1, () -> done.add("second"));       // batch of 2 is written
        assertThat(day(DAY1)).exists();
        assertThat(done).containsExactly("first", "reject-after-first", "second");
    }

    @Test
    void whenDurableRunsAtOnceWhenNothingIsWaiting() {
        BatchingSink s = sink(WriteMode.COUNT, 5, Duration.ofMinutes(5), 1000, new SftrCsvWriter(','));
        List<String> done = new java.util.ArrayList<>();
        s.whenDurable(() -> done.add("now"));
        assertThat(done).containsExactly("now");
    }

    @Test
    void callbacksWaitForTheRetryWhenTheWriteFails() throws Exception {
        BatchingSink s = sink(WriteMode.INTERVAL, 10, Duration.ofMinutes(5), 1000, new FlakyWriter(1));
        List<String> done = new java.util.ArrayList<>();
        s.append(row("GIVE"), DAY1, () -> done.add("row"));
        clock.advance(Duration.ofMinutes(5));
        s.flushIfDue();
        assertThat(done).isEmpty();                                  // failed write: offset must not be released
        clock.advance(BatchingSink.RETRY_DELAY);
        s.flushIfDue();
        assertThat(done).containsExactly("row");
    }

    @Test
    void aFailingCallbackDoesNotStopTheOthers() throws Exception {
        BatchingSink s = sink(WriteMode.COUNT, 2, Duration.ofMinutes(5), 1000, new SftrCsvWriter(','));
        List<String> done = new java.util.ArrayList<>();
        s.append(row("GIVE"), DAY1, () -> { throw new IllegalStateException("boom"); });
        s.append(row("TAKE"), DAY1, () -> done.add("second"));
        assertThat(done).containsExactly("second");
        assertThat(sides(day(DAY1))).containsExactly("GIVE", "TAKE");
    }

    @Test
    void perRecordModeIsNotABatchMode() {
        OutputProperties props = new OutputProperties("x.csv", null, null, Rollover.DAILY, KOLKATA,
                WriteMode.PER_RECORD, 10, Duration.ofMinutes(5), 1000);
        assertThatThrownBy(() -> new BatchingSink(new SftrCsvWriter(','), processor, new OutputFileResolver(props), props, clock))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void configSelectsTheBatchingSinkForCountAndInterval() {
        for (WriteMode m : new WriteMode[] {WriteMode.COUNT, WriteMode.INTERVAL}) {
            OutputProperties props = new OutputProperties(dir.resolve("o.csv").toString(), null, null, Rollover.DAILY,
                    KOLKATA, m, 10, Duration.ofMinutes(5), 1000);
            assertThat(new ReportSinkConfig().reportSink(props, new SftrCsvWriter(','), processor,
                    new OutputFileResolver(props), clock)).isInstanceOf(BatchingSink.class);
        }
    }
}
