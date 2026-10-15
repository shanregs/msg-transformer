package com.bmo.sftr.mapping.functions;

import static org.assertj.core.api.Assertions.assertThat;

import com.bmo.sftr.mapping.TestSupport;
import com.bmo.sftr.mapping.model.SftrConstants;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

class RefDataExtractorsTest {

    private final SftrConstants c = TestSupport.realConstants();

    private static JsonNode json(String s) {
        return TestSupport.MAPPER.readTree(s);
    }

    @Test
    void atReturnsValue() {
        assertThat(RefDataExtractors.at(json("{\"a\":{\"b\":\"x\"}}"), "a.b")).isEqualTo("x");
    }

    @Test
    void atReturnsNullWhenNotUsable() {
        assertThat(RefDataExtractors.at(json("{\"a\":{}}"), "a.b")).isNull();
        assertThat(RefDataExtractors.at(json("{\"a\":{\"b\":null}}"), "a.b")).isNull();
        assertThat(RefDataExtractors.at(json("{\"a\":{\"b\":{\"c\":1}}}"), "a.b")).isNull();
        assertThat(RefDataExtractors.at(json("{\"a\":{\"b\":\"  \"}}"), "a.b")).isNull();
        assertThat(RefDataExtractors.at(null, "a.b")).isNull();
    }

    @Test
    void extractorsUsePathsFromConstants() {
        JsonNode r = json("""
                {"find_sftr_party":{"values":{"lei":"L1"}},
                 "find_sftr_party_sector":{"values":{"natureOfReportingCounterparty":"C","sector":"S"}}}""");
        assertThat(RefDataExtractors.partyLei(r, c)).isEqualTo("L1");
        assertThat(RefDataExtractors.partyNature(r, c)).isEqualTo("C");
        assertThat(RefDataExtractors.partySector(r, c)).isEqualTo("S");
    }
}
