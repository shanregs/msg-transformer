package com.bmo.sftr.mapping.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Compiles the mapping file once, at startup, and shares the result. {@link RecordProcessor} takes the active
 * mappings from it and {@code SftrCsvWriter} takes the column list, so neither needs the other. A wrong rule
 * fails the start here, with every problem listed.
 */
@Configuration(proxyBeanMethods = false)
public class MappingConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MappingConfiguration.class);

    @Bean
    public MappingConfigLoader.CompiledConfig compiledConfig(MappingConfigLoader loader) {
        MappingConfigLoader.CompiledConfig cfg = loader.load();
        log.info("SFTR mappings: {} columns, {} active, {} skipped (PENDING/IGNORE/NOT_APPLICABLE)",
                cfg.columns().size(), cfg.active().size(), cfg.skipped());
        return cfg;
    }
}
