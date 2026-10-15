package com.bmo.sftr.mapping.functions;

import static org.assertj.core.api.Assertions.assertThat;

import com.bmo.sftr.mapping.TestSupport;
import com.bmo.sftr.mapping.model.MappingFunction;
import com.bmo.sftr.mapping.model.ProcessingContext;
import com.bmo.sftr.mapping.model.SftrConstants;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.JsonNode;

class SftrMappingFunctionsTest {

    private static final JsonNode EMPTY = TestSupport.MAPPER.createObjectNode();

    private final SftrConstants constants = TestSupport.realConstants();
    private final SftrMappingFunctions fns = new SftrMappingFunctions();

    private static final JsonNode REF = TestSupport.MAPPER.readTree("""
            {"find_sftr_party":{"values":{"lei":"549300ABCDEFGHIJ0001"}},
             "find_sftr_party_sector":{"values":{"natureOfReportingCounterparty":"C","sector":"CDTI"}}}""");

    private Object run(String name, String message, JsonNode ref) {
        MappingFunction fn = fns.find(name).orElseThrow();
        return fn.apply(TestSupport.MAPPER.readTree(message), new ProcessingContext(ref, constants, null));
    }

    @ParameterizedTest
    @CsvSource({"PO,GIVE", "B,GIVE", "PI,TAKE", "LC,TAKE", "XX,", "'',"})
    void cpSide(String tradeType, String expected) {
        assertThat(run("CP_CpSide", "{\"trade_type\":\"" + tradeType + "\"}", EMPTY)).isEqualTo(expected);
    }

    @Test
    void cpSideMissingTradeType() {
        assertThat(run("CP_CpSide", "{}", EMPTY)).isNull();
    }

    @Test
    void enrichmentFromRefData() {
        assertThat(run("CP_RepCp", "{}", REF)).isEqualTo("549300ABCDEFGHIJ0001");
        assertThat(run("CP_NatureOfRepCp", "{}", REF)).isEqualTo("C");
        assertThat(run("CP_SecOfRepCp", "{}", REF)).isEqualTo("CDTI");
    }

    @Test
    void natureFallsBackToDefault() {
        assertThat(run("CP_NatureOfRepCp", "{}", EMPTY)).isEqualTo(constants.value("natureOfRepCpDefault"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"LDNBB01", "LDN-1"})
    void branchIsSetForLondon(String entity) {
        assertThat(run("CP_BranchOfRepCp", "{\"bmoLegalEntity\":\"" + entity + "\"}", EMPTY))
                .isEqualTo(constants.value("londonBranchCountry"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"NYC1", "TOR", ""})
    void branchIsNullOtherwise(String entity) {
        assertThat(run("CP_BranchOfRepCp", "{\"bmoLegalEntity\":\"" + entity + "\"}", EMPTY)).isNull();
    }

    @Test
    void branchIsNullWhenMissing() {
        assertThat(run("CP_BranchOfRepCp", "{}", EMPTY)).isNull();
    }

    @Test
    void everyLambdaToleratesEmptyInputs() {
        List<String> names = List.of("CP_CpSide", "CP_RepCp", "CP_NatureOfRepCp", "CP_SecOfRepCp", "CP_BranchOfRepCp");
        for (String name : names) {
            MappingFunction fn = fns.find(name).orElseThrow();
            fn.apply(EMPTY, new ProcessingContext(EMPTY, constants, null));
        }
    }
}
