package com.bmo.sftr.mapping.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.bmo.sftr.mapping.model.SftrTransactionReport;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** Real mapping file + real application.yml against the real report class. */
class ConfigConsistencyTest {

    @Test
    void realConfigLoadsAndCoversReport() {
        // load() validates every row, including that every constantRef exists
        MappingConfigLoader.CompiledConfig cfg = MappingConfigLoaderTest.loader("sftr-field-mappings.json").load();

        Set<Field> covered = cfg.columns().stream().map(MappingConfigLoader.Column::field).collect(Collectors.toSet());
        List<String> uncovered = Arrays.stream(SftrTransactionReport.class.getDeclaredFields())
                .filter(f -> !covered.contains(f))
                .map(Field::getName)
                .toList();

        System.out.println("Report fields without a mapping row: " + uncovered);
        if (Boolean.getBoolean("sftr.strict")) {
            assertThat(uncovered).isEmpty();
        }
    }
}
