package com.bmo.sftr.mapping.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.bmo.sftr.mapping.engine.MappingConfigLoader;
import com.bmo.sftr.mapping.engine.MappingConfigLoaderTest;
import java.util.List;
import org.junit.jupiter.api.Test;

class OutputColumnsTest {

    @Test
    void constantsMatchTheMappingFileExactly() {
        List<String> fromFile = MappingConfigLoaderTest.loader("mapper/sftr-field-mappings.json").load()
                .columns().stream().map(MappingConfigLoader.Column::name).toList();

        assertThat(OutputColumns.all()).containsExactlyInAnyOrderElementsOf(fromFile);
    }

    @Test
    void constantsAreUnique() {
        assertThat(OutputColumns.all()).doesNotHaveDuplicates();
    }
}
