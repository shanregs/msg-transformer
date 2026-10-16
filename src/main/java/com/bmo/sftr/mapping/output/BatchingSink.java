package com.bmo.sftr.mapping.output;

import com.bmo.sftr.mapping.engine.RecordProcessor;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import com.bmo.sftr.mapping.output.OutputProperties.WriteMode;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Write modes {@code COUNT} and {@code INTERVAL}: rows wait in a list and are written as one batch
 * (open the file, append all, close).
 *
 * <p>A batch is written when
 * <ul>
 *   <li>{@code COUNT}: {@code batch-size} rows are waiting;</li>
 *   <li>{@code INTERVAL}: the oldest waiting row is {@code batch-interval} old (a flusher virtual thread
 *       watches the deadline, so rows are written even when no more messages arrive);</li>
 *   <li>either mode: the next row belongs to a different day (a batch never mixes two days), midnight
 *       passes in the clock's zone (rows from late evening do not wait into the next day),
 *       {@code max-buffered-records} is reached, or the sink is closed.</li>
 * </ul>
 *
 * <p>If a batch cannot be written the rows stay in the list and the write is retried after
 * {@link #RETRY_DELAY}. A write that fails halfway can leave some rows in the file and write them again
 * on the retry (at-least-once). When the list is full and cannot be written, {@link #append} throws and
 * the row is not accepted.
 *
 * <p>One lock guards the list and the file, so the caller thread and the flusher never write together.
 */
public class BatchingSink implements ReportSink {

    private static final Logger log = LoggerFactory.getLogger(BatchingSink.class);

    static final Duration RETRY_DELAY = Duration.ofSeconds(5);

    private final SftrCsvWriter csvWriter;
    private final RecordProcessor processor;
    private final OutputFileResolver files;
    private final WriteMode mode;
    private final int batchSize;
    private final Duration batchInterval;
    private final int maxBuffered;
    private final Clock clock;
    private final boolean flusherThread;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition wake = lock.newCondition();
    private final List<SftrTransactionReport> buffer = new ArrayList<>();
    private LocalDate bufferDay;        // the day of every buffered row
    private Instant firstAt;            // when the oldest buffered row arrived
    private Instant retryAt;            // set after a failed write
    private boolean closed;
    private Thread flusher;

    public BatchingSink(SftrCsvWriter csvWriter, RecordProcessor processor, OutputFileResolver files,
                        OutputProperties props, Clock clock) {
        this(csvWriter, processor, files, props, clock, true);
    }

    /** @param flusherThread false lets tests drive the timer by calling {@link #flushIfDue()} with their own clock */
    BatchingSink(SftrCsvWriter csvWriter, RecordProcessor processor, OutputFileResolver files,
                 OutputProperties props, Clock clock, boolean flusherThread) {
        if (props.writeMode() == WriteMode.PER_RECORD) {
            throw new IllegalArgumentException("BatchingSink needs write-mode COUNT or INTERVAL");
        }
        this.csvWriter = csvWriter;
        this.processor = processor;
        this.files = files;
        this.mode = props.writeMode();
        this.batchSize = props.batchSize();
        this.batchInterval = props.batchInterval();
        this.maxBuffered = props.maxBufferedRecords();
        this.clock = clock;
        this.flusherThread = flusherThread;
    }

    @Override
    public void append(SftrTransactionReport report, LocalDate day) throws IOException {
        lock.lock();
        try {
            if (closed) throw new IllegalStateException("Sink is closed");
            if (!buffer.isEmpty() && !day.equals(bufferDay)) flushLocked();     // a batch never mixes days
            if (buffer.size() >= maxBuffered) flushLocked();                    // full: make room or refuse
            if (buffer.isEmpty()) {
                bufferDay = day;
                firstAt = clock.instant();
                startFlusherLocked();
                wake.signalAll();
            }
            buffer.add(report);
            if (mode == WriteMode.COUNT && buffer.size() >= batchSize) flushLocked();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        lock.lock();
        try {
            flushLocked();
        } finally {
            lock.unlock();
        }
    }

    /** Writes the batch if its deadline has passed. Returns true if rows were written. Called by the flusher. */
    boolean flushIfDue() {
        lock.lock();
        try {
            if (buffer.isEmpty() || clock.instant().isBefore(deadlineLocked())) return false;
            flushLocked();
            return true;
        } catch (IOException | RuntimeException e) {
            log.error("Could not write {} buffered rows to {}; will retry", buffer.size(), files.report(bufferDay), e);
            return false;
        } finally {
            lock.unlock();
        }
    }

    /** Number of rows waiting to be written. */
    int buffered() {
        lock.lock();
        try {
            return buffer.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        Thread t;
        lock.lock();
        try {
            closed = true;
            wake.signalAll();
            t = flusher;
        } finally {
            lock.unlock();
        }
        if (t != null && t != Thread.currentThread()) {
            try {
                t.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        flush();
    }

    /** Must hold the lock. Writes the whole buffer to its day's file; on failure the rows stay. */
    private void flushLocked() throws IOException {
        if (buffer.isEmpty()) return;
        try (SftrCsvWriter.Session csv = csvWriter.openAppend(files.report(bufferDay), processor.columnDefs())) {
            for (SftrTransactionReport row : buffer) csv.write(row);
        } catch (IOException | RuntimeException e) {
            retryAt = clock.instant().plus(RETRY_DELAY);
            throw e;
        }
        log.debug("Wrote {} rows to {}", buffer.size(), files.report(bufferDay));
        buffer.clear();
        bufferDay = null;
        firstAt = null;
        retryAt = null;
    }

    /** Must hold the lock and have rows buffered. The earlier of the interval and midnight, pushed back after a failure. */
    private Instant deadlineLocked() {
        var zone = clock.getZone();
        Instant midnight = firstAt.atZone(zone).toLocalDate().plusDays(1).atStartOfDay(zone).toInstant();
        Instant due = mode == WriteMode.INTERVAL ? min(firstAt.plus(batchInterval), midnight) : midnight;
        return retryAt != null && retryAt.isAfter(due) ? retryAt : due;
    }

    private static Instant min(Instant a, Instant b) {
        return a.isBefore(b) ? a : b;
    }

    private void startFlusherLocked() {
        if (!flusherThread || flusher != null) return;
        flusher = Thread.ofVirtual().name("sftr-batch-flusher").unstarted(this::runFlusher);
        flusher.start();
    }

    private void runFlusher() {
        lock.lock();
        try {
            while (!closed) {
                try {
                    if (buffer.isEmpty()) {
                        wake.await();
                        continue;
                    }
                    Duration wait = Duration.between(clock.instant(), deadlineLocked());
                    if (wait.isZero() || wait.isNegative()) {
                        try {
                            flushLocked();
                        } catch (IOException | RuntimeException e) {
                            log.error("Could not write {} buffered rows to {}; will retry", buffer.size(),
                                    files.report(bufferDay), e);
                        }
                        continue;
                    }
                    wake.awaitNanos(wait.toNanos());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
