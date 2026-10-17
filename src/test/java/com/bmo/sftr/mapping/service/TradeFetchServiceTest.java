package com.bmo.sftr.mapping.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bmo.sftr.mapping.db.DbProperties;
import com.bmo.sftr.mapping.db.Trade;
import com.bmo.sftr.mapping.db.TradeEvent;
import com.bmo.sftr.mapping.db.TradeRepository;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.data.domain.Limit;

class TradeFetchServiceTest {

    private final TradeRepository repository = mock(TradeRepository.class);
    private final RecordPipeline pipeline = mock(RecordPipeline.class);

    private TradeFetchService service(int pageSize) {
        return new TradeFetchService(repository, pipeline, new DbProperties(TradeEvent.RECEIVED, pageSize));
    }

    private static Trade trade(long id) {
        return new Trade(id, "other-app", Instant.parse("2026-10-17T00:00:00Z"), TradeEvent.RECEIVED, "{\"id\":" + id + "}");
    }

    /** Serves the given ids in pages, the way the keyset query does. */
    private void tableHolds(long... ids) {
        when(repository.findByEventAndIdGreaterThanOrderByIdAsc(eq(TradeEvent.RECEIVED), any(Long.class), any(Limit.class)))
                .thenAnswer(inv -> {
                    long after = inv.getArgument(1);
                    int max = ((Limit) inv.getArgument(2)).max();
                    List<Trade> page = new ArrayList<>();
                    for (long id : ids) if (id > after && page.size() < max) page.add(trade(id));
                    return page;
                });
    }

    @Test
    void walksThroughEveryRowInIdOrderPageByPageAndFlushesOnce() throws Exception {
        tableHolds(1, 2, 3, 4, 5);

        TradeFetchService.Summary s = service(2).process(null);

        assertThat(s.read()).isEqualTo(5);
        assertThat(s.written()).isEqualTo(5);
        assertThat(s.failed()).isZero();
        assertThat(s.firstId()).isEqualTo(1);
        assertThat(s.lastId()).isEqualTo(5);
        InOrder order = inOrder(pipeline);
        for (long id = 1; id <= 5; id++) order.verify(pipeline).handle("{\"id\":" + id + "}", "trade:" + id);
        order.verify(pipeline).flush();
        verify(pipeline, times(1)).flush();
        verify(repository, times(3)).findByEventAndIdGreaterThanOrderByIdAsc(any(), any(Long.class), any());   // 2 + 2 + 1
    }

    @Test
    void aFailedRowIsCountedAndTheRunCarriesOnWithTheNextRow() throws Exception {
        tableHolds(1, 2, 3, 4);
        doThrow(new RecordProcessingException("trade:2", new IllegalStateException("Mapping failed for column X")))
                .when(pipeline).handle("{\"id\":2}", "trade:2");
        doThrow(new IOException("disk")).when(pipeline).handle("{\"id\":3}", "trade:3");
        doThrow(new IllegalArgumentException("boom")).when(pipeline).handle("{\"id\":4}", "trade:4");

        TradeFetchService.Summary s = service(10).process(null);

        assertThat(s.read()).isEqualTo(4);
        assertThat(s.written()).isEqualTo(1);
        assertThat(s.failed()).isEqualTo(3);
        verify(pipeline).flush();
    }

    @Test
    void limitStopsTheRunEvenInTheMiddleOfAPage() throws Exception {
        tableHolds(1, 2, 3, 4, 5, 6);

        TradeFetchService.Summary s = service(4).process(5);

        assertThat(s.read()).isEqualTo(5);
        assertThat(s.lastId()).isEqualTo(5);
        verify(pipeline, never()).handle(any(), eq("trade:6"));
    }

    @Test
    void anEmptyTableGivesAZeroSummaryAndStillFlushes() throws Exception {
        tableHolds();

        TradeFetchService.Summary s = service(10).process(null);

        assertThat(s.read()).isZero();
        assertThat(s.firstId()).isNull();
        verify(pipeline).flush();
    }

    @Test
    void aLimitBelowOneIsRefused() {
        assertThatThrownBy(() -> service(10).process(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aSecondRunWhileOneIsActiveIsRefusedAndTheFirstIsNotDisturbed() throws Exception {
        tableHolds(1, 2);
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        org.mockito.Mockito.doAnswer(inv -> {
            inside.countDown();
            release.await(5, TimeUnit.SECONDS);
            return null;
        }).when(pipeline).handle("{\"id\":1}", "trade:1");

        TradeFetchService service = service(10);
        Thread first = Thread.ofPlatform().start(() -> {
            try {
                service.process(null);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        assertThat(inside.await(5, TimeUnit.SECONDS)).isTrue();

        assertThatThrownBy(() -> service.process(null)).isInstanceOf(TradeFetchService.RunInProgressException.class);

        release.countDown();
        first.join(5_000);
        assertThat(service.process(null).read()).isEqualTo(2);       // free again after the first run ended
    }

    @Test
    void theRunIsFreeAgainAfterAFlushFailure() throws Exception {
        tableHolds(1);
        doThrow(new IOException("disk full")).doNothing().when(pipeline).flush();

        TradeFetchService service = service(10);
        assertThatThrownBy(() -> service.process(null)).isInstanceOf(IOException.class);
        assertThat(service.process(null).read()).isEqualTo(1);
    }
}
