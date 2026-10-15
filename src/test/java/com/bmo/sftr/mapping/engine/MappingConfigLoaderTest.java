package com.bmo.sftr.mapping.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bmo.sftr.mapping.TestSupport;
import com.bmo.sftr.mapping.functions.SftrMappingFunctions;
import com.bmo.sftr.mapping.model.MappingFunction;
import com.bmo.sftr.mapping.model.SftrConstants;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

public class MappingConfigLoaderTest {

    public static MappingConfigLoader loader(String resource, SftrMappingFunctions fns) {
        return new MappingConfigLoader(TestSupport.MAPPER, fns, TestSupport.realConstants(),
                new ClassPathResource(resource));
    }

    public static MappingConfigLoader loader(String resource) {
        return loader(resource, new SftrMappingFunctions());
    }

    @Test
    void validConfigLoads() {
        MappingConfigLoader.CompiledConfig cfg = loader("mappings/valid.json").load();
        assertThat(cfg.columns()).hasSize(5);
        assertThat(cfg.active()).hasSize(4);
        assertThat(cfg.skipped()).isEqualTo(1);
    }

    @Test
    void realConfigHasNineActiveAndSevenSkipped() {
        MappingConfigLoader.CompiledConfig cfg = loader("sftr-field-mappings.json").load();
        assertThat(cfg.columns()).hasSize(16);
        assertThat(cfg.active()).hasSize(9);
        assertThat(cfg.skipped()).isEqualTo(7);
    }

    @Test
    void allErrorsAreReportedTogether() {
        assertThatThrownBy(() -> loader("mappings/invalid-all.json").load())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("IM_SubmitterID: duplicate outputField")
                .hasMessageContaining("XX_NoSuchField: no field 'xxNoSuchField'")
                .hasMessageContaining("CP_BranchOfOtherCp: inputField is required")
                .hasMessageContaining("IM_RepCpCodeType: constantRef is required")
                .hasMessageContaining("IM_ParticipantID: Missing constant: sftr.constants.values.noSuchConstant")
                .hasMessageContaining("CP_RepCp: no lambda registered as 'NO_SUCH_HANDLER'");
    }

    @Test
    void unknownJsonPropertyFails() {
        assertThatThrownBy(() -> loader("mappings/unknown-property.json").load())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("constantValue");
    }

    @Test
    void skippedRowsAreNotValidatedBeyondColumnName() {
        MappingConfigLoader.CompiledConfig cfg = loader("mappings/pending-bad.json").load();
        assertThat(cfg.columns()).hasSize(5);
        assertThat(cfg.active()).hasSize(1);
    }

    @Test
    void skippedRowWithUnknownReportPropertyFails() {
        assertThatThrownBy(() -> loader("mappings/skipped-bad-property.json").load())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("XX_NoSuchField");
    }

    @Test
    void smokeRunFailsOnMissingConstantKey() {
        SftrMappingFunctions fns = new SftrMappingFunctions() {
            {
                register("CP_CpSide_Bad", (MappingFunction) (msg, ctx) -> ctx.constants().value("doesNotExist"));
            }
        };
        // Reuse a valid column with a handler pointing at the bad lambda.
        MappingConfigLoader l = new MappingConfigLoader(TestSupport.MAPPER, fns, TestSupport.realConstants(),
                new ClassPathResource("mappings/bad-handler-constant.json"));
        assertThatThrownBy(l::load)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing constant: sftr.constants.values.doesNotExist");
    }

    @Test
    void missingLookupTableFailsSmokeRun() {
        SftrConstants noLookups = new SftrConstants(
                java.util.Map.of("submitterId", "X", "participantId", "X", "repCpCodeType", "LEI",
                        "natureOfRepCpDefault", "F", "londonBranchCountry", "GB",
                        "refPathPartyLei", "a", "refPathPartyNature", "b", "refPathPartySector", "c"),
                java.util.Map.of("londonLegalEntityPrefixes", java.util.List.of("LDN-")),
                java.util.Map.of());
        MappingConfigLoader l = new MappingConfigLoader(TestSupport.MAPPER, new SftrMappingFunctions(), noLookups,
                new ClassPathResource("mappings/valid.json"));
        assertThatThrownBy(l::load)
                .hasMessageContaining("Missing constant: sftr.constants.lookups.cpSide");
    }

    @Test
    void propertyNames() {
        assertThat(MappingConfigLoader.propertyName("CP_RepCp")).isEqualTo("cpRepCp");
        assertThat(MappingConfigLoader.propertyName("IM_SubmitterID")).isEqualTo("imSubmitterID");
    }
}
