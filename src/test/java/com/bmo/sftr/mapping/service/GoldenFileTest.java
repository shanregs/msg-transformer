package com.bmo.sftr.mapping.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.bmo.sftr.mapping.TestSupport;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** Full application context (Kafka consumer not started) and a byte-for-byte comparison of the output. */
@SpringBootTest(properties = {"sftr.kafka.enabled=false", "sftr.output.file-date=2026-01-01"})
class GoldenFileTest {

    private static Path outDir;

    @DynamicPropertySource
    static void output(DynamicPropertyRegistry registry) throws IOException {
        outDir = Files.createTempDirectory("golden");
        registry.add("sftr.output.file", () -> outDir.resolve("out.csv").toString());
    }

    @MockitoBean
    RefDataClient refDataClient;

    @Autowired RecordPipeline pipeline;

    @Test
    void pipelineMatchesGoldenFile() throws Exception {
        when(refDataClient.fetch(any())).thenReturn(TestSupport.MAPPER.readTree("""
                {"find_sftr_party":{"values":{"lei":"549300ABCDEFGHIJ0001"}},
                 "find_sftr_party_sector":{"values":{"natureOfReportingCounterparty":"C","sector":"CDTI"}}}"""));

        String line = Files.readString(new ClassPathResource("golden/input.jsonl").getFile().toPath()).strip();
        pipeline.handle(line, "trade-messages-0@0");

        byte[] expected = new ClassPathResource("golden/expected.csv").getContentAsByteArray();
        assertThat(Files.readAllBytes(outDir.resolve("out-2026-01-01.csv"))).isEqualTo(expected);
    }
}
