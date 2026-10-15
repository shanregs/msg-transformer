package com.bmo.sftr.mapping.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DateHelpersTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "2025-12-31",
            "2025-12-31T00:00:00",
            "2025-12-31T10:30:00",
            "2025-12-31T10:30:00.123456",
            "2025-12-31T23:30:00Z",
            "2025-12-31T23:30:00-05:00",
            "2025-12-31 10:30:00",
            "  2025-12-31T10:30:00  "})
    void extractsTheDateAsWritten(String input) {
        assertThat(DateHelpers.dateOf(input)).isEqualTo(LocalDate.of(2025, 12, 31));
        assertThat(DateHelpers.dateOnly(input)).isEqualTo("2025-12-31");
    }

    @Test
    void nullAndBlankGiveNull() {
        assertThat(DateHelpers.dateOf(null)).isNull();
        assertThat(DateHelpers.dateOf("  ")).isNull();
        assertThat(DateHelpers.dateOnly(null)).isNull();
    }

    @Test
    void invalidValueThrowsWithoutEchoingIt() {
        assertThatThrownBy(() -> DateHelpers.dateOf("31/12/2025"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("31/12");
        assertThatThrownBy(() -> DateHelpers.dateOf("2025-13-45T10:00:00"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void customPatternIgnoresTimeParts() {
        assertThat(DateHelpers.dateOf("31/12/2025 10:30:00", "dd/MM/yyyy HH:mm:ss")).isEqualTo(LocalDate.of(2025, 12, 31));
        assertThat(DateHelpers.dateOf("31/12/2025", "dd/MM/yyyy")).isEqualTo(LocalDate.of(2025, 12, 31));
        assertThat(DateHelpers.dateOf(null, "dd/MM/yyyy")).isNull();
        assertThatThrownBy(() -> DateHelpers.dateOf("2025-12-31", "dd/MM/yyyy"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void comparisonsMixDateOnlyAndTimestamps() {
        // a later time on the same day is still the same date
        assertThat(DateHelpers.isEqual("2025-12-31", "2025-12-31T23:59:59")).isTrue();
        assertThat(DateHelpers.isAfter("2026-01-01T00:00:01", "2025-12-31T23:59:59")).isTrue();
        assertThat(DateHelpers.isAfter("2025-12-31T23:59:59", "2025-12-31")).isFalse();
        assertThat(DateHelpers.isBefore("2025-12-30", "2025-12-31T00:00:00")).isTrue();
        assertThat(DateHelpers.isBefore("2025-12-31", "2025-12-31")).isFalse();
        assertThat(DateHelpers.isOnOrAfter("2025-12-31T01:00:00", "2025-12-31")).isTrue();
        assertThat(DateHelpers.isOnOrAfter("2025-12-30", "2025-12-31")).isFalse();
        assertThat(DateHelpers.isOnOrBefore("2025-12-31", "2025-12-31T23:00:00")).isTrue();
        assertThat(DateHelpers.isOnOrBefore("2026-01-01", "2025-12-31")).isFalse();
    }

    @Test
    void betweenIsInclusive() {
        assertThat(DateHelpers.isBetween("2025-12-15", "2025-12-01", "2025-12-31")).isTrue();
        assertThat(DateHelpers.isBetween("2025-12-01T08:00:00", "2025-12-01", "2025-12-31")).isTrue();
        assertThat(DateHelpers.isBetween("2025-12-31", "2025-12-01", "2025-12-31T00:00:00")).isTrue();
        assertThat(DateHelpers.isBetween("2026-01-01", "2025-12-01", "2025-12-31")).isFalse();
        assertThat(DateHelpers.isBetween("2025-11-30", "2025-12-01", "2025-12-31")).isFalse();
    }

    @Test
    void missingSideMakesEveryComparisonFalse() {
        assertThat(DateHelpers.isAfter(null, "2025-12-31")).isFalse();
        assertThat(DateHelpers.isBefore("2025-12-31", " ")).isFalse();
        assertThat(DateHelpers.isEqual((String) null, (String) null)).isFalse();
        assertThat(DateHelpers.isOnOrAfter((String) null, "2025-12-31")).isFalse();
        assertThat(DateHelpers.isOnOrBefore("2025-12-31", (String) null)).isFalse();
        assertThat(DateHelpers.isBetween("2025-12-15", null, "2025-12-31")).isFalse();
    }

    @Test
    void daysBetween() {
        assertThat(DateHelpers.daysBetween("2025-12-31", "2026-01-03T10:00:00")).isEqualTo(3L);
        assertThat(DateHelpers.daysBetween("2026-01-03", "2025-12-31")).isEqualTo(-3L);
        assertThat(DateHelpers.daysBetween("2025-12-31", "2025-12-31T23:00:00")).isEqualTo(0L);
        assertThat(DateHelpers.daysBetween(null, "2025-12-31")).isNull();
    }
}
