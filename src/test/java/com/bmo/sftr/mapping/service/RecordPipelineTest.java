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

    @BeforeEach
    void setUp() {
        out = dir.resolve("out.csv");
        when(refData.fetch(any())).thenReturn(REF);
    }

    private RecordPipeline pipeline() {
        RecordProcessor processor = new RecordProcessor(MappingConfigLoaderTest.loader("mapper/sftr-field-mappings.json"));
        return new RecordPipeline(refData, processor, TestSupport.csvWriter(), TestSupport.realConstants(),
                TestSupport.MAPPER, out.toString(), "2026-01-01");
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
        p.handle(msg("1", "PO"), "t:1");
        assertThat(sides()).containsExactly("GIVE");               // written before handle() returned
        p.handle(msg("2", "PI"), "t:2");
        p.handle(msg("3", "PO"), "t:3");
        p.handle(msg("4", "PI"), "t:4");
        assertThat(sides()).containsExactly("GIVE", "TAKE", "GIVE", "TAKE");
    }

    @Test
    void badMessageThrowsWithAShortReasonAndTheNextOneStillFollowsInOrder() throws Exception {
        RecordPipeline p = pipeline();
        p.handle(msg("1", "PO"), "t:1");
        assertThatThrownBy(() -> p.handle("{not json SECRET-DATA", "t:2"))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessage("Malformed JSON")                      // never the content
                .extracting(e -> ((RecordProcessingException) e).sourceRef()).isEqualTo("t:2");
        assertThatThrownBy(() -> p.handle("", "t:3")).isInstanceOf(RecordProcessingException.class)
                .hasMessageStartingWith("Empty");
        assertThatThrownBy(() -> p.handle(null, "t:4")).isInstanceOf(RecordProcessingException.class);
        assertThatThrownBy(() -> p.handle("[1,2]", "t:5")).isInstanceOf(RecordProcessingException.class)
                .hasMessageStartingWith("Not a JSON");
        p.handle(msg("6", "PI"), "t:6");

        assertThat(sides()).containsExactly("GIVE", "TAKE");
        assertThat(dir.resolve("out.csv.rejects")).doesNotExist();   // there is no rejects file
    }

    @Test
    void referenceData4xxFailsTheRecord() {
        when(refData.fetch(any())).thenThrow(HttpClientErrorException.create(
                HttpStatus.BAD_REQUEST, "Bad Request", null, "body with data".getBytes(), null));
        assertThatThrownBy(() -> pipeline().handle(msg("1", "PO"), "t:1"))
                .isInstanceOf(RecordProcessingException.class).hasMessage("Reference data HTTP 400");
    }

    @Test
    void referenceDataOutageFailsThatRecordAndProcessingContinues() throws Exception {
        RecordPipeline p = pipeline();
        p.handle(msg("1", "PO"), "t:1");

        when(refData.fetch(any())).thenThrow(new RefDataClient.UnavailableException("down", null));
        assertThatThrownBy(() -> p.handle(msg("2", "PI"), "t:2"))
                .isInstanceOf(RecordProcessingException.class).hasMessage("Reference data unavailable");

        doReturn(REF).when(refData).fetch(any());                  // service is back: next record is fine
        p.handle(msg("3", "PO"), "t:3");
        assertThat(sides()).containsExactly("GIVE", "GIVE");
    }

    @Test
    void restartAppendsWithoutASecondHeader() throws Exception {
        RecordPipeline first = pipeline();
        first.handle(msg("1", "PO"), "t:1");
        first.close();

        RecordPipeline second = pipeline();
        second.handle(msg("2", "PI"), "t:2");
        second.close();

        List<String> lines = Files.readAllLines(out);
        assertThat(lines).hasSize(3);
        assertThat(lines.stream().filter(l -> l.startsWith("IM_SubmitterID,")).count()).isEqualTo(1);
        assertThat(sides()).containsExactly("GIVE", "TAKE");
    }
}
