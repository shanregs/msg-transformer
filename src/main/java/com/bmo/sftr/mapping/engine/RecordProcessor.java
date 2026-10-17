package com.bmo.sftr.mapping.engine;

import com.bmo.sftr.mapping.model.ProcessingContext;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/** Runs the active mappings for one message. The column list belongs to the CSV writer, not to this class. */
@Component
public class RecordProcessor {

    private final List<MappingConfigLoader.CompiledMapping> mappings;      // active rows only

    @Autowired
    public RecordProcessor(MappingConfigLoader.CompiledConfig config) {
        this.mappings = config.active();
    }

    /** Compiles the mapping file itself; for tests that do not use the Spring-managed {@code CompiledConfig}. */
    public RecordProcessor(MappingConfigLoader loader) {
        this(loader.load());
    }

    public SftrTransactionReport process(JsonNode message, ProcessingContext ctx) {
        SftrTransactionReport report = new SftrTransactionReport();
        for (MappingConfigLoader.CompiledMapping m : mappings) {
            try {
                m.set(report, m.value(message, ctx));
            } catch (Exception e) {
                throw new IllegalStateException("Mapping failed for column " + m.column(), e);
            }
        }
        return report;
    }
}
