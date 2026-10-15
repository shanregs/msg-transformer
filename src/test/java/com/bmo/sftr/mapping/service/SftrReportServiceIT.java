package com.bmo.sftr.mapping.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.bmo.sftr.mapping.TestSupport;
import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.model.SftrConstants;
import com.bmo.sftr.mapping.output.SftrCsvWriter;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@SpringBootTest(properties = "sftr.processing.max-concurrent-ref-data-calls=5")
class SftrReportServiceIT {

    private static final JsonNode REF = TestSupport.MAPPER.readTree("""
            {"find_sftr_party":{"values":{"lei":"549300ABCDEFGHIJ0001"}},
             "find_sftr_party_sector":{"values":{"natureOfReportingCounterparty":"C","sector":"CDTI"}}}""");

    private static final int MALFORMED_LINE = 7;
    private static final int FAILING_LINE = 20;

    @MockitoBean
    RefDataClient refDataClient;

    @Autowired SftrReportService service;
    @Autowired RecordProcessor processor;
    @Autowired SftrCsvWriter csvWriter;
    @Autowired SftrConstants constants;
    @Autowired ObjectMapper mapper;

    @TempDir
    Path dir;

    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger maxInFlight = new AtomicInteger();

    @BeforeEach
    void stub() {
        when(refDataClient.fetch(any())).thenAnswer(inv -> {
            JsonNode msg = inv.getArgument(0);
            int now = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(now, Math::max);
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(0, 6));
                if ("FAIL".equals(msg.get("trade_ref").asText())) {
                    throw new IllegalStateException("Reference data call failed after 3 attempts");
                }
                return REF;
            } finally {
                inFlight.decrementAndGet();
            }
        });
    }

    private Path input() throws Exception {
        List<String> lines = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            if (i == MALFORMED_LINE) {
                lines.add("{not json SECRET-DATA");
            } else if (i == FAILING_LINE) {
                lines.add("{\"trade_ref\":\"FAIL\",\"trade_type\":\"PO\"}");
            } else {
                lines.add("{\"trade_ref\":\"T" + i + "\",\"trade_type\":\"" + (i % 2 == 0 ? "PO" : "PI")
                        + "\",\"bmoLegalEntity\":\"LDNBB" + i + "\"}");
            }
        }
        Path f = dir.resolve("input.jsonl");
        Files.write(f, lines);
        return f;
    }

    private SftrReportService serviceWithLimit(int limit) {
        return new SftrReportService(refDataClient, processor, csvWriter, constants, mapper, 10, limit);
    }

    @Test
    void rejectsBadRecordsAndKeepsInputOrder() throws Exception {
        Path out = dir.resolve("out.csv");
        SftrReportService.RunResult r = service.run(input(), out, "2026-01-01");

        assertThat(r).isEqualTo(new SftrReportService.RunResult(50, 48, 2));
        List<String> csv = Files.readAllLines(out);
        assertThat(csv).hasSize(49);

        List<String> rejects = Files.readAllLines(dir.resolve("out.csv.rejects"));
        assertThat(rejects).hasSize(2);
        assertThat(rejects.get(0)).startsWith(MALFORMED_LINE + "\t");
        assertThat(rejects.get(1)).startsWith(FAILING_LINE + "\t");
        assertThat(String.join("\n", rejects)).doesNotContain("SECRET-DATA").doesNotContain("FAIL\"");

        // input order: sides alternate PI(odd)=TAKE, PO(even)=GIVE; line 7 and 20 are gone
        List<String> sides = csv.stream().skip(1).map(l -> l.split(",", -1)[12]).toList();
        List<String> expected = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            if (i == MALFORMED_LINE || i == FAILING_LINE) continue;
            expected.add(i % 2 == 0 ? "GIVE" : "TAKE");
        }
        assertThat(sides).isEqualTo(expected);
    }

    @Test
    void concurrencyIsLimited() throws Exception {
        service.run(input(), dir.resolve("limited.csv"), "2026-01-01");
        assertThat(maxInFlight.get()).isLessThanOrEqualTo(5).isGreaterThan(0);
    }

    @Test
    void outputIsIdenticalForDifferentLimits() throws Exception {
        Path in = input();
        Path a = dir.resolve("a.csv");
        Path b = dir.resolve("b.csv");
        serviceWithLimit(1).run(in, a, "2026-01-01");
        serviceWithLimit(50).run(in, b, "2026-01-01");
        assertThat(Files.readAllBytes(a)).isEqualTo(Files.readAllBytes(b));
    }

    @Test
    void noRejectsFileWhenNothingRejected() throws Exception {
        Path in = dir.resolve("clean.jsonl");
        Files.writeString(in, "{\"trade_ref\":\"T1\",\"trade_type\":\"PO\"}\n\n");
        SftrReportService.RunResult r = service.run(in, dir.resolve("clean.csv"), "2026-01-01");
        assertThat(r).isEqualTo(new SftrReportService.RunResult(1, 1, 0));
        assertThat(dir.resolve("clean.csv.rejects")).doesNotExist();
    }
}
