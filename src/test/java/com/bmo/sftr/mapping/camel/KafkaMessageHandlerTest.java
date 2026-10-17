package com.bmo.sftr.mapping.camel;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.bmo.sftr.mapping.service.RecordPipeline;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

class KafkaMessageHandlerTest {

    private final RecordPipeline pipeline = mock(RecordPipeline.class);
    private final KafkaMessageHandler handler = new KafkaMessageHandler(pipeline, new DefaultCamelContext());

    private Exchange exchange(long offset, String body) {
        Exchange ex = new DefaultExchange(new DefaultCamelContext());
        ex.getIn().setBody(body);
        ex.getIn().setHeader(KafkaConstants.TOPIC, "trade-messages");
        ex.getIn().setHeader(KafkaConstants.PARTITION, 0);
        ex.getIn().setHeader(KafkaConstants.OFFSET, offset);
        return ex;
    }

    @Test
    void passesTheMessageAndItsSourceReferenceToThePipeline() throws Exception {
        handler.handle(exchange(7, "{\"a\":1}"));
        verify(pipeline).handle(eq("{\"a\":1}"), eq("trade-messages-0@7"));
    }

    @Test
    void aFailureIsLoggedAndTheRouteCarriesOn() throws Exception {
        doThrow(new java.io.IOException("disk")).when(pipeline).handle(any(), any());
        handler.handle(exchange(3, "{}"));                     // must not throw
        handler.handle(exchange(4, "{}"));                     // the next message is still handled
        verify(pipeline).handle(eq("{}"), eq("trade-messages-0@4"));
    }
}
