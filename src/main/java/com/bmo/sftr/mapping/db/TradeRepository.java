package com.bmo.sftr.mapping.db;

import java.util.List;
import org.springframework.data.domain.Limit;
import org.springframework.data.repository.Repository;
import org.springframework.transaction.annotation.Transactional;

/** Read-only access to the trade table. */
@Transactional(readOnly = true)
public interface TradeRepository extends Repository<Trade, Long> {

    /** The next rows of an event after the given id, in id order: one page of a keyset walk through the table. */
    List<Trade> findByEventAndIdGreaterThanOrderByIdAsc(TradeEvent event, long afterId, Limit limit);

    long countByEvent(TradeEvent event);
}
