package com.bmo.sftr.mapping.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.bmo.sftr.mapping.TestSupport;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import com.bmo.sftr.mapping.service.TradeFetchService;
import jakarta.persistence.EntityManager;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.support.TransactionTemplate;

/** The seeder fills an empty table with 10 RECEIVED rows, once, and a run over them writes all of them. */
@SpringBootTest(properties = {"sftr.db.seed.enabled=true", "sftr.db.seed.count=10",
        "spring.datasource.url=jdbc:h2:mem:seedtest;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "sftr.output.file-date=2026-01-01", "sftr.output.write-mode=COUNT", "sftr.output.batch-size=100"})
@ActiveProfiles("test")
class TradeSeederTest {

    private static Path outDir;

    @DynamicPropertySource
    static void output(DynamicPropertyRegistry registry) throws IOException {
        outDir = Files.createTempDirectory("seed");
        registry.add("sftr.output.file", () -> outDir.resolve("report.csv").toString());
    }

    @MockitoBean
    RefDataClient refDataClient;

    @Autowired TradeSeeder seeder;
    @Autowired TradeFetchService service;
    @Autowired EntityManager em;
    @Autowired TransactionTemplate tx;

    @BeforeEach
    void ref() throws IOException {
        when(refDataClient.fetch(any())).thenReturn(TestSupport.MAPPER.readTree("""
                {"find_sftr_party":{"values":{"lei":"549300ABCDEFGHIJ0001"}},
                 "find_sftr_party_sector":{"values":{"natureOfReportingCounterparty":"C","sector":"CDTI"}}}"""));
    }

    private long rows() {
        Long count = tx.execute(s -> em.createQuery("select count(t) from Trade t", Long.class).getSingleResult());
        return count == null ? 0 : count;
    }

    @Test
    void startUpSeedsTenReceivedRowsAndRunningAgainAddsNone() {
        assertThat(rows()).isEqualTo(10);
        Long received = tx.execute(s -> em.createQuery("select count(t) from Trade t where t.event = :e", Long.class)
                .setParameter("e", TradeEvent.RECEIVED).getSingleResult());
        assertThat(received).isEqualTo(10L);

        seeder.run(null);                                    // the table is not empty any more: nothing happens
        assertThat(rows()).isEqualTo(10);
    }

    @Test
    void aRunOverTheSeededRowsWritesAllTenWithoutFailures() throws Exception {
        TradeFetchService.Summary s = service.process(null);

        assertThat(s.read()).isEqualTo(10);
        assertThat(s.written()).isEqualTo(10);
        assertThat(s.failed()).isZero();
        assertThat(Files.readAllLines(outDir.resolve("report-2026-01-01.csv"))).hasSize(11);   // header + 10 rows
    }

    @Test
    void theSampleTradesAreValidJsonWithTheFieldsTheMappingsNeed() {
        List<String> all = java.util.stream.IntStream.rangeClosed(1, 10).mapToObj(TradeSeeder::json).toList();
        for (String json : all) {
            var node = TestSupport.MAPPER.readTree(json);
            assertThat(node.isObject()).isTrue();
            assertThat(node.has("trade_type")).isTrue();
            assertThat(node.has("bmoLegalEntity")).isTrue();
            assertThat(node.has("counterpartyId")).isTrue();
        }
        assertThat(all.get(9)).contains("NONATURE");
        assertThat(all).doesNotHaveDuplicates();
    }
}
