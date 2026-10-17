package com.bmo.sftr.mapping.publisher;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/** Builds the JSON for message number n. Values are taken in turn from the configured lists, so a run is repeatable. */
@Component
public class TradeMessageGenerator {

    private final ObjectMapper mapper;
    private final PublisherProperties props;

    public TradeMessageGenerator(ObjectMapper mapper, PublisherProperties props) {
        this.mapper = mapper;
        this.props = props;
    }

    /** @param n 1 for the first message */
    public String message(long n) {
        long i = n - 1;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("trade_ref", "PUB-" + n);
        m.put("trade_type", pick(props.tradeTypes(), i));
        m.put("bmoLegalEntity", pick(props.legalEntities(), i));
        m.put("counterpartyId", pick(props.counterpartyIds(), i));
        return mapper.writeValueAsString(m);
    }

    private static String pick(List<String> values, long i) {
        return values.get((int) (i % values.size()));
    }
}
