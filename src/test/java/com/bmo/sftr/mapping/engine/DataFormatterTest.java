package com.bmo.sftr.mapping.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bmo.sftr.mapping.TestSupport;
import com.bmo.sftr.mapping.functions.SftrMappingFunctions;
import com.bmo.sftr.mapping.model.ProcessingContext;
import com.bmo.sftr.mapping.model.SftrConstants;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import tools.jackson.databind.JsonNode;

class DataFormatterTest {

    private final SftrConstants constants = TestSupport.bindConstants("formatter-constants.yml");

    private MappingConfigLoader loader(String file) {
        return new MappingConfigLoader(TestSupport.MAPPER, new SftrMappingFunctions(), constants,
                new ClassPathResource(file));
    }

    private SftrTransactionReport run(String json) {
        RecordProcessor rp = new RecordProcessor(loader("mappings/formatters.json"));
        JsonNode msg = TestSupport.MAPPER.readTree(json);
        return rp.process(msg, new ProcessingContext(TestSupport.MAPPER.createObjectNode(), constants, null));
    }

    @Test
    void convertsNumbersAndDates() {
        SftrTransactionReport r = run("""
                {"amount":"1,234.5","qty":"42","tradeDate":"31/12/2025","execTs":"31/12/2025 10:30:00"}""");
        assertThat(r.getImNatureOfOtherCp()).isEqualTo("1234.5000");
        assertThat(r.getImOtherCpType()).isEqualTo("42");
        assertThat(r.getCpBranchOfOtherCp()).isEqualTo("2025-12-31");
        assertThat(r.getCpSecOfRepCp()).isEqualTo("2025-12-31T15:30:00Z");   // New York -> UTC
    }

    @Test
    void missingOrBlankInputGivesNull() {
        SftrTransactionReport r = run("{\"amount\":\"  \",\"qty\":null}");
        assertThat(r.getImNatureOfOtherCp()).isNull();
        assertThat(r.getImOtherCpType()).isNull();
        assertThat(r.getCpBranchOfOtherCp()).isNull();
        assertThat(r.getCpSecOfRepCp()).isNull();
    }

    @Test
    void badValueFailsWithColumnAndWithoutEchoingTheValue() {
        assertThatThrownBy(() -> run("{\"qty\":\"4.5\"}"))
                .hasMessage("Mapping failed for column IM_OtherCpType");
        assertThatThrownBy(() -> run("{\"tradeDate\":\"2025-12-31\"}"))
                .hasMessage("Mapping failed for column CP_BranchOfOtherCp");
        assertThatThrownBy(() -> run("{\"amount\":\"abc\"}"))
                .hasMessage("Mapping failed for column IM_NatureOfOtherCp");
    }

    @Test
    void configErrorsAreCollectedAndSkippedRowsIgnored() {
        assertThatThrownBy(() -> loader("mappings/formatters-invalid.json").load())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("IM_NatureOfOtherCp: inputField is required")
                .hasMessageContaining("IM_OtherCpType: formatter is required")
                .hasMessageContaining("CP_BranchOfOtherCp: Missing constant: sftr.constants.values.noSuchRef")
                .hasMessageContaining("CP_SecOfRepCp:")
                .satisfies(e -> assertThat(e.getMessage()).doesNotContain("CP_CpSide"));
    }
}
