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
 * End to end against a real broker (localhost:19092): the Camel route, the pipeline and a COUNT sink of 3.
 * Offsets must be committed only after the batch is in the CSV.
 */
@EnabledIfSystemProperty(named = "kafka.it", matches = "true")
@SpringBootTest(properties = {"sftr.output.write-mode=COUNT", "sftr.output.batch-size=3",
        "sftr.output.rollover=NONE"})
class CamelCountModeKafkaTest {

    static final String TOPIC = "count-" + System.nanoTime();
    static final String GROUP = "g-" + TOPIC;
    static Path dir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) throws Exception {
        dir = Files.createTempDirectory("count-it");
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
    void nothingIsWrittenOrCommittedUntilTheBatchIsFullThenBothHappen() throws Exception {
        Path out = dir.resolve("report.csv");

        KafkaIngestTestSupport.send(TOPIC, message(1), message(2));
        Thread.sleep(4_000);                                         // consumed, but only 2 of 3 rows
        assertThat(out).doesNotExist();
        assertThat(committed(GROUP, TOPIC)).isEqualTo(-1);           // offsets must not run ahead of the file

        KafkaIngestTestSupport.send(TOPIC, message(3));              // third row completes the batch
        assertThat(awaitLines(out, 4, 20_000)).as("header and three rows").isTrue();
        assertThat(Files.readAllLines(out)).hasSize(4);
        assertThat(awaitCommitted(GROUP, TOPIC, 3, 20_000)).isEqualTo(3);
    }
}
