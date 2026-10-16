package com.bmo.sftr.mapping.service;

import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.model.ProcessingContext;
import com.bmo.sftr.mapping.model.SftrConstants;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import java.time.LocalDate;
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

    public MessageTransformer(RefDataClient refData, RecordProcessor processor, SftrConstants constants,
                              ObjectMapper mapper) {
        this.refData = refData;
        this.processor = processor;
        this.constants = constants;
        this.mapper = mapper;
    }

    /** @param day the date the message belongs to; written into the row as the file date */
    public SftrTransactionReport transform(String json, LocalDate day) {
        if (json == null || json.isBlank()) throw new IllegalArgumentException("Empty message");
        JsonNode message = mapper.readTree(json);
        if (message == null || !message.isObject()) throw new IllegalArgumentException("Not a JSON object");
        JsonNode ref = refData.fetch(message);
        return processor.process(message, new ProcessingContext(ref, constants, day.toString()));
    }
}
