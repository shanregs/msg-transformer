package com.bmo.sftr.mapping.camel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Holds Kafka offsets whose rows are safely on disk until the consumer thread commits them.
 *
 * <p>A Kafka consumer may only be used by its own thread (the spike showed that committing from another
 * thread throws {@code ConcurrentModificationException}). Any thread may therefore {@link #release} an
 * offset, for example the batch flusher after a timed write, but only the consumer thread calls
 * {@link #commitReleased()}, right after each message and so at the latest when the next message arrives.
 *
 * <p>Per partition only the highest released offset is kept: committing it covers every lower one.
 */
@Component
public class OffsetCommitter {

    private static final Logger log = LoggerFactory.getLogger(OffsetCommitter.class);

    private record Released(long offset, Runnable commit) {}

    private final Map<String, Released> latest = new ConcurrentHashMap<>();

    /** Marks an offset as safe to commit. {@code partition} identifies topic and partition, for example {@code t-0}. */
    public void release(String partition, long offset, Runnable commit) {
        latest.merge(partition, new Released(offset, commit), (old, next) -> next.offset() >= old.offset() ? next : old);
    }

    /** Commits every released offset. Call only on the consumer thread. Returns how many commits were made. */
    public int commitReleased() {
        if (latest.isEmpty()) return 0;
        List<Released> toCommit = new ArrayList<>();
        for (String partition : new ArrayList<>(latest.keySet())) {
            Released r = latest.remove(partition);
            if (r != null) toCommit.add(r);
        }
        int committed = 0;
        for (Released r : toCommit) {
            try {
                r.commit().run();
                committed++;
            } catch (RuntimeException e) {
                log.error("Offset commit failed; the messages will be read again after a restart", e);
            }
        }
        return committed;
    }

    /** Offsets released but not yet committed. */
    public int waiting() {
        return latest.size();
    }
}
