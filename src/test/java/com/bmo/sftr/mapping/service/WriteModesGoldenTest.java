package com.bmo.sftr.mapping.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bmo.sftr.mapping.TestSupport;
import com.bmo.sftr.mapping.engine.MappingConfigLoaderTest;
import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.output.OutputFileResolver;
import com.bmo.sftr.mapping.output.OutputProperties;
import com.bmo.sftr.mapping.output.OutputProperties.Rollover;
import com.bmo.sftr.mapping.output.OutputProperties.WriteMode;
import com.bmo.sftr.mapping.output.RejectWriter;
import com.bmo.sftr.mapping.output.ReportDay;
import com.bmo.sftr.mapping.output.ReportSinkConfig;
import com.bmo.sftr.mapping.output.SftrCsvWriter;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.ClassPathResource;

/** The same input gives the same CSV whichever write mode is used. */
class WriteModesGoldenTest {

    private static final ZoneId KOLKATA = ZoneId.of("Asia/Kolkata");
    private static final int MESSAGES = 7;

    @TempDir
    Path dir;

    private String run(WriteMode mode) throws Exception {
        RefDataClient refData = mock(RefDataClient.class);
        when(refData.fetch(any())).thenReturn(TestSupport.MAPPER.readTree("""
                {"find_sftr_party":{"values":{"lei":"549300ABCDEFGHIJ0001"}},
                 "find_sftr_party_sector":{"values":{"natureOfReportingCounterparty":"C","sector":"CDTI"}}}"""));
        RecordProcessor processor = new RecordProcessor(MappingConfigLoaderTest.loader("sftr-field-mappings.json"));
        Path out = dir.resolve(mode + ".csv");
        OutputProperties props = new OutputProperties(out.toString(), null, "2026-01-01", Rollover.NONE, KOLKATA,
                mode, 3, Duration.ofMinutes(30), 1000);
        Clock clock = Clock.system(KOLKATA);
        OutputFileResolver files = new OutputFileResolver(props);
        var sink = new ReportSinkConfig().reportSink(props, new SftrCsvWriter(','), processor, files, clock);
        RecordPipeline pipeline = new RecordPipeline(
                new MessageTransformer(refData, processor, TestSupport.realConstants(), TestSupport.MAPPER),
                sink, new RejectWriter(files), new ReportDay(props, clock));

        String line = Files.readString(new ClassPathResource("golden/input.jsonl").getFile().toPath()).strip();
        List<String> done = new ArrayList<>();
        for (int i = 0; i < MESSAGES; i++) pipeline.handle(line, "t-0@" + i, () -> done.add("x"));
        pipeline.flush();                                  // end of run: write what is still waiting
        sink.close();
        assertThat(done).as("every message is reported durable once").hasSize(MESSAGES);
        return Files.readString(out);
    }

    @Test
    void perRecordCountAndIntervalProduceTheSameFile() throws Exception {
        String expectedRow = Files.readAllLines(new ClassPathResource("golden/expected.csv").getFile().toPath()).get(1);
        String header = Files.readAllLines(new ClassPathResource("golden/expected.csv").getFile().toPath()).get(0);
        String expected = header + "\n" + (expectedRow + "\n").repeat(MESSAGES);

        String perRecord = run(WriteMode.PER_RECORD);
        assertThat(perRecord).isEqualTo(expected);
        assertThat(run(WriteMode.COUNT)).isEqualTo(perRecord);
        assertThat(run(WriteMode.INTERVAL)).isEqualTo(perRecord);
    }
}
