package com.bmo.sftr.mapping.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class KafkaPublishControllerTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(
            new KafkaPublishController(template, "trade-messages", Duration.ofMillis(300))).build();

    private static final String MSG = "{\"trade_ref\":\"1\",\"trade_type\":\"PO\"}";

    @SuppressWarnings("unchecked")
    private void acknowledged(String topic, int partition, long offset) {
        SendResult<String, String> result = mock(SendResult.class);
        when(result.getRecordMetadata()).thenReturn(
                new RecordMetadata(new TopicPartition(topic, partition), offset, 0, 0L, 0, 0));
        when(template.send(eq(topic), any(), any())).thenReturn(CompletableFuture.completedFuture(result));
    }

    @Test
    void sendsToTheConsumerTopicByDefaultAndAnswersWithPartitionAndOffset() throws Exception {
        acknowledged("trade-messages", 0, 41);

        mvc.perform(post("/api/kafka-message").contentType(MediaType.APPLICATION_JSON).content(MSG))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.topic").value("trade-messages"))
                .andExpect(jsonPath("$.partition").value(0))
                .andExpect(jsonPath("$.offset").value(41));

        verify(template).send("trade-messages", null, MSG);          // sent exactly as received, no key
    }

    @Test
    void aTopicAndKeyCanBeGiven() throws Exception {
        acknowledged("other-topic", 2, 7);

        mvc.perform(post("/api/kafka-message").param("topic", " other-topic ").param("key", "k1")
                        .contentType(MediaType.APPLICATION_JSON).content(MSG))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.topic").value("other-topic"))
                .andExpect(jsonPath("$.partition").value(2));

        verify(template).send("other-topic", "k1", MSG);
    }

    @Test
    void anEmptyBodyIsRefusedAndNothingIsSent() throws Exception {
        mvc.perform(post("/api/kafka-message").contentType(MediaType.APPLICATION_JSON).content("  "))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Empty message"));

        verify(template, never()).send(any(String.class), any(), any());
    }

    @Test
    void whenKafkaFailsTheAnswerIs503AndDoesNotEchoTheMessage() throws Exception {
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException("no broker"));
        when(template.send(eq("trade-messages"), any(), any())).thenReturn(failed);

        mvc.perform(post("/api/kafka-message").contentType(MediaType.APPLICATION_JSON).content(MSG))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value(org.hamcrest.Matchers.containsString("trade-messages")))
                .andExpect(jsonPath("$.error").value(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("trade_ref"))));
    }

    @Test
    void anAcknowledgementThatNeverComesIsA503AfterTheTimeout() throws Exception {
        when(template.send(eq("trade-messages"), any(), any())).thenReturn(new CompletableFuture<>());

        mvc.perform(post("/api/kafka-message").contentType(MediaType.APPLICATION_JSON).content(MSG))
                .andExpect(status().isServiceUnavailable());
    }

    @Test
    void aProducerThatThrowsInsteadOfFailingTheFutureIsA503() throws Exception {
        when(template.send(eq("trade-messages"), any(), any()))
                .thenThrow(new org.apache.kafka.common.KafkaException("max.block.ms exceeded"));

        mvc.perform(post("/api/kafka-message").contentType(MediaType.APPLICATION_JSON).content(MSG))
                .andExpect(status().isServiceUnavailable());
    }
}
