package com.bmo.sftr.mapping.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bmo.sftr.mapping.TestSupport;
import com.bmo.sftr.mapping.functions.SftrMappingFunctions;
import com.bmo.sftr.mapping.model.ProcessingContext;
import com.bmo.sftr.mapping.model.SftrConstants;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

class RecordProcessorTest {

    private final SftrConstants constants = TestSupport.realConstants();

    private static final JsonNode REF = TestSupport.MAPPER.readTree("""
            {"find_sftr_party":{"values":{"lei":"549300ABCDEFGHIJ0001"}},
             "find_sftr_party_sector":{"values":{"natureOfReportingCounterparty":"C","sector":"CDTI"}}}""");

    private ProcessingContext ctx(JsonNode ref) {
        return new ProcessingContext(ref, constants, "2026-01-01");
    }

    @Test
    void seedConfigSetsActiveFieldsAndLeavesSkippedNull() {
        RecordProcessor rp = new RecordProcessor(MappingConfigLoaderTest.loader("sftr-field-mappings.json"));
        JsonNode msg = TestSupport.MAPPER.readTree(
                "{\"trade_type\":\"PO\",\"bmoLegalEntity\":\"LDNBB01\",\"cparty_branch_cd\":\"X\"}");

        SftrTransactionReport r = rp.process(msg, ctx(REF));

        assertThat(r.getImSubmitterID()).isEqualTo("BMOLN9000003");
        assertThat(r.getImParticipantID()).isEqualTo("BMOLN9000003");
        assertThat(r.getImRepCpCodeType()).isEqualTo("LEI");
        assertThat(r.getCpRepCp()).isEqualTo("549300ABCDEFGHIJ0001");
        assertThat(r.getCpNatureOfRepCp()).isEqualTo("C");
        assertThat(r.getCpSecOfRepCp()).isEqualTo("CDTI");
        assertThat(r.getCpBranchOfRepCp()).isEqualTo("GB");
        assertThat(r.getCpCpSide()).isEqualTo("GIVE");
        assertThat(r.getCpEntResponsibleForRep()).isEqualTo("549300ABCDEFGHIJ0001");

        assertThat(r.getImNatureOfOtherCp()).isNull();
        assertThat(r.getCpAdditionalSecClass()).isNull();
        assertThat(r.getImBranchOfRepCpType()).isNull();
        assertThat(r.getImBranchOfOtherCpType()).isNull();
        assertThat(r.getCpBranchOfOtherCp()).isNull();      // IGNORE row: input is not read
        assertThat(r.getImOtherCptyEntResponsibleForRep()).isNull();
        assertThat(r.getImOtherCpType()).isNull();
    }

    @Test
    void directRowCopiesValueAndAbsentFieldIsNull() {
        RecordProcessor rp = new RecordProcessor(MappingConfigLoaderTest.loader("mappings/direct.json"));
        JsonNode present = TestSupport.MAPPER.readTree("{\"cparty_branch_cd\":\"BR1\"}");
        JsonNode absent = TestSupport.MAPPER.readTree("{}");

        assertThat(rp.process(present, ctx(REF)).getCpBranchOfOtherCp()).isEqualTo("BR1");
        assertThat(rp.process(absent, ctx(REF)).getCpBranchOfOtherCp()).isNull();
    }

    @Test
    void pendingRowNeverRunsItsLambda() {
        AtomicInteger calls = new AtomicInteger();
        SftrMappingFunctions fns = new SftrMappingFunctions() {
            {
                register("COUNTING", (msg, c) -> {
                    calls.incrementAndGet();
                    return "x";
                });
            }
        };
        RecordProcessor rp = new RecordProcessor(MappingConfigLoaderTest.loader("mappings/counting.json", fns));
        calls.set(0);   // ignore any startup activity
        rp.process(TestSupport.MAPPER.readTree("{\"trade_type\":\"PO\"}"), ctx(REF));
        assertThat(calls).hasValue(0);
    }

    @Test
    void throwingLambdaNamesTheColumn() {
        SftrMappingFunctions fns = new SftrMappingFunctions() {
            {
                register("THROWS", (msg, c) -> {
                    if (msg.has("trade_type")) throw new IllegalArgumentException("boom");
                    return null;     // tolerate the empty smoke-run message
                });
            }
        };
        RecordProcessor rp = new RecordProcessor(MappingConfigLoaderTest.loader("mappings/throwing.json", fns));
        assertThatThrownBy(() -> rp.process(TestSupport.MAPPER.readTree("{\"trade_type\":\"PO\"}"), ctx(REF)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Mapping failed for column CP_CpSide");
    }
}
