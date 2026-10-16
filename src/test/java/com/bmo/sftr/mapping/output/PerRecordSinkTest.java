package com.bmo.sftr.mapping.output;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bmo.sftr.mapping.engine.MappingConfigLoaderTest;
import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import com.bmo.sftr.mapping.output.OutputProperties.WriteMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PerRecordSinkTest {

    @TempDir
    Path dir;

    private final RecordProcessor processor = new RecordProcessor(MappingConfigLoaderTest.loader("sftr-field-mappings.json"));

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
        SftrCsvWriter writer = spy(new SftrCsvWriter(','));
        PerRecordSink sink = new PerRecordSink(writer, processor, out);

        sink.append(row("GIVE"));
        sink.append(row("TAKE"));
        sink.append(row("GIVE"));

        verify(writer, times(3)).openAppend(any(), any());
        assertThat(sides(out)).containsExactly("GIVE", "TAKE", "GIVE");
    }

    @Test
    void headerIsWrittenOnceAndTheFileIsNotLeftOpen() throws Exception {
        Path out = dir.resolve("out.csv");
        PerRecordSink sink = new PerRecordSink(new SftrCsvWriter(','), processor, out);
        sink.append(row("GIVE"));
        sink.append(row("TAKE"));

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
        new PerRecordSink(new SftrCsvWriter(','), processor, out).append(row("GIVE"));
        new PerRecordSink(new SftrCsvWriter(','), processor, out).append(row("TAKE"));
        assertThat(Files.readAllLines(out)).hasSize(3);
        assertThat(sides(out)).containsExactly("GIVE", "TAKE");
    }

    @Test
    void flushAndCloseAreHarmless() throws Exception {
        PerRecordSink sink = new PerRecordSink(new SftrCsvWriter(','), processor, dir.resolve("out.csv"));
        sink.flush();
        sink.close();
        sink.append(row("GIVE"));
        sink.flush();
        sink.close();
        assertThat(sides(dir.resolve("out.csv"))).containsExactly("GIVE");
    }

    @Test
    void perRecordModeSelectsThisSink() {
        OutputProperties props = new OutputProperties(dir.resolve("o.csv").toString(), null, null,
                WriteMode.PER_RECORD, 10, Duration.ofMinutes(5), 1000);
        assertThat(new ReportSinkConfig().reportSink(props, new SftrCsvWriter(','), processor))
                .isInstanceOf(PerRecordSink.class);
    }

    @Test
    void batchModesAreNotAvailableYet() {
        OutputProperties count = new OutputProperties("o.csv", null, null, WriteMode.COUNT, 10, Duration.ofMinutes(5), 1000);
        assertThatThrownBy(() -> new ReportSinkConfig().reportSink(count, new SftrCsvWriter(','), processor))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("COUNT");
    }
}
