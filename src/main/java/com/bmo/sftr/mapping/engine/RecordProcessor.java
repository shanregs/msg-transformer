package com.bmo.sftr.mapping.engine;

import com.bmo.sftr.mapping.model.ProcessingContext;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

@Component
public class RecordProcessor {

    private static final Logger log = LoggerFactory.getLogger(RecordProcessor.class);

    private final List<MappingConfigLoader.Column> columns;                // every row, config order
    private final List<MappingConfigLoader.CompiledMapping> mappings;      // active rows only

    public RecordProcessor(MappingConfigLoader loader) {
        MappingConfigLoader.CompiledConfig cfg = loader.load();
        this.columns = cfg.columns();
        this.mappings = cfg.active();
        log.info("SFTR mappings: {} columns, {} active, {} skipped (PENDING/IGNORE/NOT_APPLICABLE)",
                columns.size(), mappings.size(), cfg.skipped());
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

    public List<MappingConfigLoader.Column> columnDefs() {
        return columns;
    }
}
