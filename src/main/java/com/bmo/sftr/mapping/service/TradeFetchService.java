package com.bmo.sftr.mapping.service;

import com.bmo.sftr.mapping.db.DbProperties;
import com.bmo.sftr.mapping.db.Trade;
import com.bmo.sftr.mapping.db.TradeEvent;
import com.bmo.sftr.mapping.db.TradeRepository;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;

/**
 * Walks through the RECEIVED rows of the trade table in id order, a page at a time, and hands each row's JSON
 * to {@link RecordPipeline}. A row that fails is logged (its id and a short reason, never the JSON) and the walk
 * carries on with the next row. At the end the pipeline is flushed so batched rows reach the CSV.
 *
 * <p>Only one run can be active at a time. Rows the other application inserts while the run is walking the
 * table are picked up if their id is higher than the row being processed; a run ends at the first short page.
 */
@Service
public class TradeFetchService {

    /** @param read rows fetched; @param written rows handed to the CSV writer; @param failed rows that failed */
    public record Summary(TradeEvent event, long read, long written, long failed, Long firstId, Long lastId,
                          long durationMs) {}

    /** Another run is still going. */
    public static class RunInProgressException extends RuntimeException {
        public RunInProgressException() {
            super("A run is already in progress");
        }
    }

    private static final Logger log = LoggerFactory.getLogger(TradeFetchService.class);

    private final TradeRepository repository;
    private final RecordPipeline pipeline;
    private final DbProperties props;
    private final AtomicBoolean running = new AtomicBoolean();

    public TradeFetchService(TradeRepository repository, RecordPipeline pipeline, DbProperties props) {
        this.repository = repository;
        this.pipeline = pipeline;
        this.props = props;
    }

    /**
     * @param limit stop after this many rows; {@code null} means all of them
     * @throws RunInProgressException another run is active
     * @throws IOException            the final flush of the CSV failed
     */
    public Summary process(Integer limit) throws IOException {
        if (limit != null && limit < 1) throw new IllegalArgumentException("limit must be at least 1");
        if (!running.compareAndSet(false, true)) throw new RunInProgressException();
        long started = System.nanoTime();
        long read = 0, written = 0, failed = 0;
        Long firstId = null, lastId = null;
        try {
            long after = Long.MIN_VALUE;
            log.info("Run started: event {}, page size {}, limit {}", props.event(), props.pageSize(),
                    limit == null ? "none" : limit);
            while (limit == null || read < limit) {
                int size = limit == null ? props.pageSize() : (int) Math.min(props.pageSize(), limit - read);
                List<Trade> page = repository.findByEventAndIdGreaterThanOrderByIdAsc(props.event(), after, Limit.of(size));
                for (Trade trade : page) {
                    Long id = trade.getId();
                    after = id;
                    if (firstId == null) firstId = id;
                    lastId = id;
                    read++;
                    try {
                        pipeline.handle(trade.getDataJson(), "trade:" + id);
                        written++;
                    } catch (RecordProcessingException e) {
                        failed++;
                        log.error("Trade {} failed: {}", id, e.getMessage());
                    } catch (IOException | RuntimeException e) {
                        failed++;
                        log.error("Trade {} could not be written: {}", id, e.toString());
                    }
                }
                if (page.size() < size) break;                  // a short page was the last one
            }
            pipeline.flush();
            long ms = (System.nanoTime() - started) / 1_000_000;
            log.info("Run finished: read {}, written {}, failed {} in {} ms", read, written, failed, ms);
            return new Summary(props.event(), read, written, failed, firstId, lastId, ms);
        } finally {
            running.set(false);
        }
    }
}
