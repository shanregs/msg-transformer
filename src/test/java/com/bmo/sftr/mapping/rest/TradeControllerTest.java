package com.bmo.sftr.mapping.rest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.bmo.sftr.mapping.db.TradeEvent;
import com.bmo.sftr.mapping.service.TradeFetchService;
import com.bmo.sftr.mapping.output.OutputFileResolver;
import com.bmo.sftr.mapping.output.OutputProperties;
import com.bmo.sftr.mapping.output.OutputProperties.Rollover;
import com.bmo.sftr.mapping.output.OutputProperties.WriteMode;
import com.bmo.sftr.mapping.output.ReportDay;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class TradeControllerTest {

    private static final ZoneId KOLKATA = ZoneId.of("Asia/Kolkata");

    private final TradeFetchService service = mock(TradeFetchService.class);
    private final OutputProperties props = new OutputProperties("data/r.csv", "2026-10-16", Rollover.DAILY, KOLKATA,
            WriteMode.COUNT, 100, Duration.ofMinutes(5), 1000);
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new TradeController(service,
            new ReportDay(props, Clock.system(KOLKATA)), new OutputFileResolver(props))).build();

    @Test
    void answersWithTheSummaryAndTodaysCsvFile() throws Exception {
        when(service.process(null)).thenReturn(new TradeFetchService.Summary(TradeEvent.RECEIVED, 10, 8, 2, 1L, 12L, 345));

        mvc.perform(post("/api/trades/process"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.event").value("RECEIVED"))
                .andExpect(jsonPath("$.read").value(10))
                .andExpect(jsonPath("$.written").value(8))
                .andExpect(jsonPath("$.failed").value(2))
                .andExpect(jsonPath("$.firstId").value(1))
                .andExpect(jsonPath("$.lastId").value(12))
                .andExpect(jsonPath("$.durationMs").value(345))
                .andExpect(jsonPath("$.reportFile").value(org.hamcrest.Matchers.endsWith("r-2026-10-16.csv")));
    }

    @Test
    void passesTheLimitOn() throws Exception {
        when(service.process(5)).thenReturn(new TradeFetchService.Summary(TradeEvent.RECEIVED, 5, 5, 0, 1L, 5L, 1));

        mvc.perform(post("/api/trades/process").param("limit", "5")).andExpect(status().isOk());

        verify(service).process(5);
    }

    @Test
    void aRunAlreadyActiveAnswers409() throws Exception {
        when(service.process(null)).thenThrow(new TradeFetchService.RunInProgressException());

        mvc.perform(post("/api/trades/process"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("A run is already in progress"));
    }

    @Test
    void aBadLimitAnswers400() throws Exception {
        when(service.process(0)).thenThrow(new IllegalArgumentException("limit must be at least 1"));

        mvc.perform(post("/api/trades/process").param("limit", "0"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("limit must be at least 1"));
    }

    @Test
    void aFailureInTheDatabaseOrTheCsvAnswers500WithoutDetails() throws Exception {
        when(service.process(null)).thenThrow(new IOException("C:\\secret\\path\\disk full"));

        mvc.perform(post("/api/trades/process"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("Run failed: IOException"));
    }
}
