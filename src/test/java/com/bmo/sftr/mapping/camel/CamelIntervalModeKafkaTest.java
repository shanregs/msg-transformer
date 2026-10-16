package com.bmo.sftr.mapping.camel;

import static com.bmo.sftr.mapping.camel.KafkaIngestTestSupport.BROKERS;
import static com.bmo.sftr.mapping.camel.KafkaIngestTestSupport.awaitCommitted;
import static com.bmo.sftr.mapping.camel.KafkaIngestTestSupport.awaitLines;
import static com.bmo.sftr.mapping.camel.KafkaIngestTestSupport.committed;
import static com.bmo.sftr.mapping.camel.KafkaIngestTestSupport.message;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.bmo.sftr.mapping.TestSupport;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * End to end against a real broker: INTERVAL sink of 3 seconds. The timer thread writes the CSV on its own;
 * the offset follows only when the next message arrives, because only the consumer thread may commit.
 */
@EnabledIfSystemProperty(named = "kafka.it", matches = "true")
@SpringBootTest(properties = {"sftr.output.write-mode=INTERVAL", "sftr.output.batch-interval=3s",
        "sftr.output.rollover=NONE"})
class CamelIntervalModeKafkaTest {

    static final String TOPIC = "interval-" + System.nanoTime();
    static final String GROUP = "g-" + TOPIC;
    static Path dir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) throws Exception {
        dir = Files.createTempDirectory("interval-it");
        KafkaIngestTestSupport.createTopic(TOPIC);
        r.add("sftr.kafka.topic", () -> TOPIC);
        r.add("spring.kafka.bootstrap-servers", () -> BROKERS);
        r.add("spring.kafka.consumer.group-id", () -> GROUP);
        r.add("sftr.output.file", () -> dir.resolve("report.csv").toString());
    }

    @MockitoBean
    RefDataClient refDataClient;

    @BeforeEach
    void ref() throws IOException {
        when(refDataClient.fetch(any())).thenReturn(TestSupport.MAPPER.readTree("""
                {"find_sftr_party":{"values":{"lei":"549300ABCDEFGHIJ0001"}},
                 "find_sftr_party_sector":{"values":{"natureOfReportingCounterparty":"C","sector":"CDTI"}}}"""));
    }

    @Test
    void theTimerWritesTheBatchAndTheNextMessageReleasesTheOffsets() throws Exception {
        Path out = dir.resolve("report.csv");

        KafkaIngestTestSupport.send(TOPIC, message(1), message(2));
        assertThat(awaitLines(out, 3, 30_000)).as("header and two rows, written by the timer").isTrue();
        Thread.sleep(2_000);
        assertThat(committed(GROUP, TOPIC)).as("flusher thread cannot commit").isEqualTo(-1);

        KafkaIngestTestSupport.send(TOPIC, message(3));              // next message: consumer thread commits 1 and 2
        assertThat(awaitCommitted(GROUP, TOPIC, 2, 20_000)).isEqualTo(2);
        assertThat(awaitLines(out, 4, 30_000)).as("third row written by the next timer").isTrue();
    }
}
