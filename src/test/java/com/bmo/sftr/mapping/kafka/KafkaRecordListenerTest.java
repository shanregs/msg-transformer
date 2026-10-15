package com.bmo.sftr.mapping.kafka;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bmo.sftr.mapping.refdata.RefDataClient;
import com.bmo.sftr.mapping.service.RecordPipeline;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.kafka.support.Acknowledgment;

class KafkaRecordListenerTest {

    private final RecordPipeline pipeline = mock(RecordPipeline.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);
    private final KafkaRecordListener listener = new KafkaRecordListener(pipeline);

    private static ConsumerRecord<String, String> record(long offset, String value) {
        return new ConsumerRecord<>("trade-messages", 0, offset, "k", value);
    }

    @Test
    void acknowledgesOnlyAfterTheMessageIsHandled() throws Exception {
        listener.onMessage(record(7, "{}"), ack);

        InOrder order = inOrder(pipeline, ack);
        order.verify(pipeline).handle("{}", "trade-messages-0@7");
        order.verify(ack).acknowledge();
    }

    @Test
    void rejectedMessageIsAlsoAcknowledged() throws Exception {
        when(pipeline.handle("bad", "trade-messages-0@8")).thenReturn(RecordPipeline.Outcome.REJECTED);
        listener.onMessage(record(8, "bad"), ack);
        verify(ack).acknowledge();
    }

    @Test
    void failureIsNotAcknowledgedSoTheSameMessageIsRedelivered() throws Exception {
        when(pipeline.handle("{}", "trade-messages-0@9"))
                .thenThrow(new RefDataClient.UnavailableException("down", null));

        assertThatThrownBy(() -> listener.onMessage(record(9, "{}"), ack))
                .isInstanceOf(RefDataClient.UnavailableException.class);
        verify(ack, never()).acknowledge();
    }

    @Test
    void tombstoneIsHandedOverAsNull() throws Exception {
        listener.onMessage(record(10, null), ack);
        verify(pipeline).handle(null, "trade-messages-0@10");
        verify(ack).acknowledge();
    }
}
