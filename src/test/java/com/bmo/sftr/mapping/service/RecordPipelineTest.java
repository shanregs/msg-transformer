package com.bmo.sftr.mapping.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bmo.sftr.mapping.TestSupport;
import com.bmo.sftr.mapping.engine.MappingConfigLoaderTest;
import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.output.SftrCsvWriter;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import tools.jackson.databind.JsonNode;

class RecordPipelineTest {

    private static final JsonNode REF = TestSupport.MAPPER.readTree("""
            {"find_sftr_party":{"values":{"lei":"549300ABCDEFGHIJ0001"}},
             "find_sftr_party_sector":{"values":{"natureOfReportingCounterparty":"C","sector":"CDTI"}}}""");

    @TempDir
    Path dir;

    private final RefDataClient refData = mock(RefDataClient.class);
    private Path out;
    private Path rejects;

    @BeforeEach
    void setUp() {
        out = dir.resolve("out.csv");
        rejects = dir.resolve("out.csv.rejects");
        when(refData.fetch(any())).thenReturn(REF);
    }

    private RecordPipeline pipeline() {
        RecordProcessor processor = new RecordProcessor(MappingConfigLoaderTest.loader("sftr-field-mappings.json"));
        return new RecordPipeline(refData, processor, new SftrCsvWriter(','), TestSupport.realConstants(),
                TestSupport.MAPPER, out.toString(), "", true, "2026-01-01");
    }

    private static String msg(String ref, String tradeType) {
        return "{\"trade_ref\":\"" + ref + "\",\"trade_type\":\"" + tradeType + "\",\"bmoLegalEntity\":\"LDNBB01\"}";
    }

    /** The CpSide column (13th) of every data row. */
    private List<String> sides() throws Exception {
        return Files.readAllLines(out).stream().skip(1).map(l -> l.split(",", -1)[12]).toList();
    }

    @Test
    void rowsAreWrittenInArrivalOrderAndVisibleImmediately() throws Exception {
        RecordPipeline p = pipeline();
        assertThat(p.handle(msg("1", "PO"), "t-0@0")).isEqualTo(RecordPipeline.Outcome.WRITTEN);
        assertThat(sides()).containsExactly("GIVE");               // flushed before handle() returned
        p.handle(msg("2", "PI"), "t-0@1");
        p.handle(msg("3", "PO"), "t-0@2");
        p.handle(msg("4", "PI"), "t-0@3");
        assertThat(sides()).containsExactly("GIVE", "TAKE", "GIVE", "TAKE");
        assertThat(rejects).doesNotExist();
    }

    @Test
    void badMessageIsRejectedAndTheNextOneStillFollowsInOrder() throws Exception {
        RecordPipeline p = pipeline();
        p.handle(msg("1", "PO"), "t-0@0");
        assertThat(p.handle("{not json SECRET-DATA", "t-0@1")).isEqualTo(RecordPipeline.Outcome.REJECTED);
        assertThat(p.handle("", "t-0@2")).isEqualTo(RecordPipeline.Outcome.REJECTED);
        assertThat(p.handle(null, "t-0@3")).isEqualTo(RecordPipeline.Outcome.REJECTED);
        assertThat(p.handle("[1,2]", "t-0@4")).isEqualTo(RecordPipeline.Outcome.REJECTED);
        p.handle(msg("6", "PI"), "t-0@5");

        assertThat(sides()).containsExactly("GIVE", "TAKE");
        List<String> lines = Files.readAllLines(rejects);
        assertThat(lines).hasSize(4);
        assertThat(lines.get(0)).isEqualTo("t-0@1\tMalformed JSON");
        assertThat(lines.get(1)).startsWith("t-0@2\t");
        assertThat(String.join("\n", lines)).doesNotContain("SECRET-DATA");
    }

    @Test
    void referenceData4xxRejectsTheMessage() throws Exception {
        when(refData.fetch(any())).thenThrow(HttpClientErrorException.create(
                HttpStatus.BAD_REQUEST, "Bad Request", null, "body with data".getBytes(), null));
        assertThat(pipeline().handle(msg("1", "PO"), "t-0@0")).isEqualTo(RecordPipeline.Outcome.REJECTED);
        assertThat(Files.readString(rejects)).isEqualTo("t-0@0\tReference data HTTP 400\n");
    }

    @Test
    void referenceDataOutageIsNotSkippedAndRetryKeepsTheOrder() throws Exception {
        RecordPipeline p = pipeline();
        p.handle(msg("1", "PO"), "t-0@0");

        when(refData.fetch(any())).thenThrow(new RefDataClient.UnavailableException("down", null));
        assertThatThrownBy(() -> p.handle(msg("2", "PI"), "t-0@1"))
                .isInstanceOf(RefDataClient.UnavailableException.class);
        assertThat(sides()).containsExactly("GIVE");              // nothing written for the failed message
        assertThat(rejects).doesNotExist();                        // and it is not treated as bad

        doReturn(REF).when(refData).fetch(any());                  // service is back: the SAME message is retried
        p.handle(msg("2", "PI"), "t-0@1");
        p.handle(msg("3", "PO"), "t-0@2");
        assertThat(sides()).containsExactly("GIVE", "TAKE", "GIVE");
    }

    @Test
    void restartAppendsWithoutASecondHeader() throws Exception {
        RecordPipeline first = pipeline();
        first.handle(msg("1", "PO"), "t-0@0");
        first.close();

        RecordPipeline second = pipeline();
        second.handle(msg("2", "PI"), "t-0@1");
        second.close();

        List<String> lines = Files.readAllLines(out);
        assertThat(lines).hasSize(3);
        assertThat(lines.stream().filter(l -> l.startsWith("IM_SubmitterID,")).count()).isEqualTo(1);
        assertThat(sides()).containsExactly("GIVE", "TAKE");
    }
}
