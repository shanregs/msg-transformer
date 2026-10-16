package com.bmo.sftr.mapping.service;

import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.model.ProcessingContext;
import com.bmo.sftr.mapping.model.SftrConstants;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import java.time.LocalDate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Turns one message into one report row: parse, fetch reference data, run the mappings.
 * Throws if the message cannot be processed; the caller decides what to do with it.
 */
@Component
public class MessageTransformer {

    private final RefDataClient refData;
    private final RecordProcessor processor;
    private final SftrConstants constants;
    private final ObjectMapper mapper;
    private final String configuredFileDate;

    public MessageTransformer(RefDataClient refData, RecordProcessor processor, SftrConstants constants,
                              ObjectMapper mapper,
                              @Value("${sftr.output.file-date:}") String configuredFileDate) {
        this.refData = refData;
        this.processor = processor;
        this.constants = constants;
        this.mapper = mapper;
        this.configuredFileDate = configuredFileDate;
    }

    public SftrTransactionReport transform(String json) {
        if (json == null || json.isBlank()) throw new IllegalArgumentException("Empty message");
        JsonNode message = mapper.readTree(json);
        if (message == null || !message.isObject()) throw new IllegalArgumentException("Not a JSON object");
        JsonNode ref = refData.fetch(message);
        String fileDate = (configuredFileDate == null || configuredFileDate.isBlank())
                ? LocalDate.now().toString() : configuredFileDate;
        return processor.process(message, new ProcessingContext(ref, constants, fileDate));
    }
}
