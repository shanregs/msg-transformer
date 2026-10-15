package com.bmo.sftr.mapping.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.bmo.sftr.mapping.TestSupport;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class GoldenFileTest {

    @MockitoBean
    RefDataClient refDataClient;

    @Autowired SftrReportService service;

    @TempDir
    Path dir;

    @Test
    void pipelineMatchesGoldenFile() throws Exception {
        when(refDataClient.fetch(any())).thenReturn(TestSupport.MAPPER.readTree("""
                {"find_sftr_party":{"values":{"lei":"549300ABCDEFGHIJ0001"}},
                 "find_sftr_party_sector":{"values":{"natureOfReportingCounterparty":"C","sector":"CDTI"}}}"""));

        Path out = dir.resolve("out.csv");
        service.run(new ClassPathResource("golden/input.jsonl").getFile().toPath(), out, "2026-01-01");

        byte[] expected = new ClassPathResource("golden/expected.csv").getContentAsByteArray();
        assertThat(Files.readAllBytes(out)).isEqualTo(expected);
    }
}
