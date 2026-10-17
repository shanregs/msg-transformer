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
import java.time.Instant;
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

/**
 * The whole path with a real (in-memory) database: trade table, repository, service, pipeline, CSV file.
 * RECEIVED rows are processed; other events are not; a bad row is skipped and nothing else is written.
 */
@SpringBootTest(properties = {"sftr.output.file-date=2026-01-01", "sftr.output.write-mode=COUNT",
        "spring.datasource.url=jdbc:h2:mem:flowtest;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "sftr.output.batch-size=100", "sftr.db.page-size=2"})
@ActiveProfiles("test")
class TradeProcessingFlowTest {

    private static Path outDir;

    @DynamicPropertySource
    static void output(DynamicPropertyRegistry registry) throws IOException {
        outDir = Files.createTempDirectory("db-flow");
        registry.add("sftr.output.file", () -> outDir.resolve("report.csv").toString());
    }

    @MockitoBean
    RefDataClient refDataClient;

    @Autowired TradeFetchService service;
    @Autowired EntityManager em;
    @Autowired TransactionTemplate tx;

    private static String json(String tradeType) {
        return "{\"trade_type\":\"" + tradeType + "\",\"bmoLegalEntity\":\"LDNBB01\",\"counterpartyId\":\"CP1\"}";
    }

    private void insert(TradeEvent event, String dataJson) {      // the database assigns ids 1, 2, 3 ... in this order
        tx.executeWithoutResult(s -> em.persist(new Trade("other-app", Instant.parse("2026-10-17T00:00:00Z"), event, dataJson)));
    }

    @BeforeEach
    void ref() throws IOException {
        when(refDataClient.fetch(any())).thenReturn(TestSupport.MAPPER.readTree("""
                {"find_sftr_party":{"values":{"lei":"549300ABCDEFGHIJ0001"}},
                 "find_sftr_party_sector":{"values":{"natureOfReportingCounterparty":"C","sector":"CDTI"}}}"""));
    }

    @Test
    void onlyReceivedRowsAreProcessedABadRowIsSkippedAndTheCsvHoldsTheGoodOnes() throws Exception {
        insert(TradeEvent.RECEIVED, json("PO"));
        insert(TradeEvent.PROCESSED, json("PI"));          // not RECEIVED: ignored
        insert(TradeEvent.RECEIVED, "{\"trade_type\":\"PI\"}".replace("PI", "PI"));
        insert(TradeEvent.RECEIVED, "[1,2,3]");            // valid JSON, not an object: fails
        insert(TradeEvent.PUBLISHED, json("PO"));          // ignored
        insert(TradeEvent.RECEIVED, json("PI"));

        TradeFetchService.Summary s = service.process(null);

        assertThat(s.event()).isEqualTo(TradeEvent.RECEIVED);
        assertThat(s.read()).isEqualTo(4);                    // ids 1, 3, 4, 6 (page size 2: two pages)
        assertThat(s.written()).isEqualTo(3);
        assertThat(s.failed()).isEqualTo(1);
        assertThat(s.firstId()).isEqualTo(1);
        assertThat(s.lastId()).isEqualTo(6);

        List<String> lines = Files.readAllLines(outDir.resolve("report-2026-01-01.csv"));
        assertThat(lines).hasSize(4);                         // header + 3 rows, written by the final flush
        assertThat(lines.stream().skip(1).map(l -> l.split(",", -1)[12]).toList())
                .containsExactly("GIVE", "TAKE", "TAKE");     // rows 1, 3, 6 in id order
        try (var files = Files.list(outDir)) {
            assertThat(files.map(p -> p.getFileName().toString()).toList())
                    .noneMatch(n -> n.endsWith(".rejects"));  // no rejects file
        }
    }
}
