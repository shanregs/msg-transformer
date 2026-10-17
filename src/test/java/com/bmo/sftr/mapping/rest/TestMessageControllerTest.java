package com.bmo.sftr.mapping.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
import com.bmo.sftr.mapping.service.MessageTransformer;
import com.bmo.sftr.mapping.service.RecordPipeline;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/** The endpoint driving the real pipeline and a real CSV file. */
class TestMessageControllerTest {

    private static final ZoneId KOLKATA = ZoneId.of("Asia/Kolkata");

    @TempDir
    Path dir;

    private MockMvc mvc(WriteMode mode) throws Exception {
        RefDataClient refData = mock(RefDataClient.class);
        when(refData.fetch(any())).thenReturn(TestSupport.MAPPER.readTree("""
                {"find_sftr_party":{"values":{"lei":"549300ABCDEFGHIJ0001"}},
                 "find_sftr_party_sector":{"values":{"natureOfReportingCounterparty":"C","sector":"CDTI"}}}"""));
        RecordProcessor processor = new RecordProcessor(MappingConfigLoaderTest.loader("mapper/sftr-field-mappings.json"));
        OutputProperties props = new OutputProperties(dir.resolve("report.csv").toString(), null, "2026-10-16",
                Rollover.DAILY, KOLKATA, mode, 100, Duration.ofMinutes(30), 1000);
        Clock clock = Clock.system(KOLKATA);
        OutputFileResolver files = new OutputFileResolver(props);
        var sink = new ReportSinkConfig().reportSink(props, new SftrCsvWriter(','), processor, files, clock);
        ReportDay day = new ReportDay(props, clock);
        RecordPipeline pipeline = new RecordPipeline(
                new MessageTransformer(refData, processor, TestSupport.realConstants(), TestSupport.MAPPER),
                sink, new RejectWriter(files), day);
        return MockMvcBuilders.standaloneSetup(new TestMessageController(pipeline, day, files)).build();
    }

    private static final String MSG = "{\"trade_ref\":\"1\",\"trade_type\":\"PO\",\"bmoLegalEntity\":\"LDNBB01\"}";

    @Test
    void aMessageGoesThroughTheFlowAndIsInTheCsvWhenTheCallReturns() throws Exception {
        mvc(WriteMode.PER_RECORD).perform(post("/api/test-message").contentType(MediaType.APPLICATION_JSON).content(MSG))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("WRITTEN"))
                .andExpect(jsonPath("$.reportFile").value(org.hamcrest.Matchers.endsWith("report-2026-10-16.csv")))
                .andExpect(jsonPath("$.rejectsFile").doesNotExist());

        Path csv = dir.resolve("report-2026-10-16.csv");
        assertThat(Files.readAllLines(csv)).hasSize(2);
        assertThat(Files.readAllLines(csv).get(1)).contains("BMOLN9000003").contains("GIVE");
    }

    @Test
    void aBadMessageAnswers200WithRejectedAndTheRejectsFile() throws Exception {
        mvc(WriteMode.PER_RECORD).perform(post("/api/test-message").contentType(MediaType.APPLICATION_JSON).content("{not json"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("REJECTED"))
                .andExpect(jsonPath("$.rejectsFile").value(org.hamcrest.Matchers.endsWith("report-2026-10-16.csv.rejects")));

        assertThat(Files.readString(dir.resolve("report-2026-10-16.csv.rejects"))).isEqualTo("rest-1\tMalformed JSON\n");
    }

    @Test
    void anEmptyBodyIsRejectedNotAnError() throws Exception {
        mvc(WriteMode.PER_RECORD).perform(post("/api/test-message")).andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("REJECTED"));
    }

    @Test
    void flushIsOnByDefaultSoABatchedRowIsWrittenAtOnce() throws Exception {
        MockMvc mvc = mvc(WriteMode.COUNT);                       // batch of 100: would wait without the flush
        mvc.perform(post("/api/test-message").contentType(MediaType.APPLICATION_JSON).content(MSG))
                .andExpect(jsonPath("$.flushed").value(true));
        assertThat(Files.readAllLines(dir.resolve("report-2026-10-16.csv"))).hasSize(2);
    }

    @Test
    void flushFalseLeavesTheRowWaitingInTheBatch() throws Exception {
        MockMvc mvc = mvc(WriteMode.COUNT);
        mvc.perform(post("/api/test-message?flush=false").contentType(MediaType.APPLICATION_JSON).content(MSG))
                .andExpect(jsonPath("$.outcome").value("WRITTEN"))
                .andExpect(jsonPath("$.flushed").value(false));
        assertThat(dir.resolve("report-2026-10-16.csv")).doesNotExist();
    }
}
