package com.bmo.sftr.mapping.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bmo.sftr.mapping.TestSupport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SftrConstantsTest {

    private final SftrConstants c = TestSupport.realConstants();

    @Test
    void bindsFromYaml() {
        assertThat(c.value("repCpCodeType")).isEqualTo("LEI");
        assertThat(c.list("londonLegalEntityPrefixes")).containsExactly("LDNBB", "LDN-");
    }

    @Test
    void lookupResolvesCodes() {
        assertThat(c.lookup("cpSide", "PO")).isEqualTo("GIVE");
        assertThat(c.lookup("cpSide", "LC")).isEqualTo("TAKE");
        assertThat(c.lookup("cpSide", "ZZ")).isNull();
        assertThat(c.lookup("cpSide", null)).isNull();
    }

    @Test
    void missingConstantsThrow() {
        assertThatThrownBy(() -> c.lookup("nope", "PO")).isInstanceOf(SftrConstants.MissingConstantException.class);
        assertThatThrownBy(() -> c.lookup("nope", null)).isInstanceOf(SftrConstants.MissingConstantException.class);
        assertThatThrownBy(() -> c.value("nope")).isInstanceOf(SftrConstants.MissingConstantException.class);
        assertThatThrownBy(() -> c.list("nope")).isInstanceOf(SftrConstants.MissingConstantException.class);
    }

    @Test
    void codeUnderTwoResultsFailsConstruction() {
        Map<String, Map<String, List<String>>> lookups =
                Map.of("t", Map.of("GIVE", List.of("PO"), "TAKE", List.of("PO")));
        assertThatThrownBy(() -> new SftrConstants(Map.of(), Map.of(), lookups))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PO");
    }

    @Test
    void startsWithAny() {
        assertThat(c.startsWithAny("londonLegalEntityPrefixes", "LDNBB01")).isTrue();
        assertThat(c.startsWithAny("londonLegalEntityPrefixes", "LDN-X")).isTrue();
        assertThat(c.startsWithAny("londonLegalEntityPrefixes", "NYC1")).isFalse();
        assertThat(c.startsWithAny("londonLegalEntityPrefixes", null)).isFalse();
    }
}
