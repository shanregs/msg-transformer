package com.bmo.sftr.mapping.camel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.bmo.sftr.mapping.service.RecordPipeline;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

class KafkaMessageHandlerTest {

    private final RecordPipeline pipeline = mock(RecordPipeline.class);
    private final OffsetCommitter committer = new OffsetCommitter();
    private final KafkaMessageHandler handler =
            new KafkaMessageHandler(pipeline, committer, new DefaultCamelContext());
    private final OffsetCommitProcessor commit = new OffsetCommitProcessor(committer);
    private final KafkaManualCommit manualCommit = mock(KafkaManualCommit.class);

    private Exchange exchange(long offset, String body) {
        Exchange ex = new DefaultExchange(new DefaultCamelContext());
        ex.getIn().setBody(body);
        ex.getIn().setHeader(KafkaConstants.TOPIC, "trade-messages");
        ex.getIn().setHeader(KafkaConstants.PARTITION, 0);
        ex.getIn().setHeader(KafkaConstants.OFFSET, offset);
        ex.getIn().setHeader(KafkaConstants.MANUAL_COMMIT, manualCommit);
        return ex;
    }

    /** The two route steps: the bean, then the process. */
    private void step(Exchange ex) {
        handler.handle(ex);
        commit.process(ex);
    }

    /** Makes the mocked pipeline behave like a sink that has already written the row. */
    private void rowIsWrittenImmediately() throws Exception {
        doAnswer(inv -> {
            ((Runnable) inv.getArgument(2)).run();
            return RecordPipeline.Outcome.WRITTEN;
        }).when(pipeline).handle(any(), any(), any());
    }

    @Test
    void passesTheMessageAndItsSourceReferenceToThePipeline() throws Exception {
        step(exchange(7, "{\"a\":1}"));
        verify(pipeline).handle(eq("{\"a\":1}"), eq("trade-messages-0@7"), any());
    }

    @Test
    void commitsRightAfterTheMessageWhenItsRowIsAlreadyOnDisk() throws Exception {
        rowIsWrittenImmediately();
        step(exchange(7, "{}"));
        verify(manualCommit).commit();
        assertThat(committer.waiting()).isZero();
    }

    @Test
    void doesNotCommitWhileTheRowIsStillWaitingInABatch() throws Exception {
        step(exchange(7, "{}"));                       // pipeline mock never calls the callback
        verify(manualCommit, never()).commit();
    }

    @Test
    void anOffsetReleasedByAnotherThreadIsCommittedWhenTheNextMessageArrives() throws Exception {
        Runnable[] held = new Runnable[1];
        doAnswer(inv -> {
            held[0] = inv.getArgument(2);                      // the row waits in a batch
            return RecordPipeline.Outcome.WRITTEN;
        }).when(pipeline).handle(eq("{}"), eq("trade-messages-0@1"), any());
        step(exchange(1, "{}"));
        verify(manualCommit, never()).commit();

        Thread.ofVirtual().start(held[0]).join();              // the flusher thread writes the batch and releases
        verify(manualCommit, never()).commit();                // still nothing: not the consumer thread

        step(exchange(2, "{}"));                       // next message: consumer thread commits
        verify(manualCommit).commit();
    }

    @Test
    void aFailureIsLoggedAndTheRouteCarriesOn() throws Exception {
        doAnswer(inv -> { throw new java.io.IOException("disk"); }).when(pipeline).handle(any(), any(), any());
        step(exchange(3, "{}"));                       // must not throw
        verify(manualCommit, never()).commit();
    }
}
