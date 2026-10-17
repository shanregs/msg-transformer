package com.bmo.sftr.mapping.db;

import jakarta.persistence.EntityManager;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Fills an EMPTY trade table with sample RECEIVED rows so that {@code POST /api/trades/process} has something to
 * work on. For local development only: the real table belongs to another application.
 *
 * <p>Runs at start-up when {@code sftr.db.seed.enabled=true} (the {@code seed} profile). If the table already
 * has any row the seeder does nothing, so it can stay switched on without piling up rows. All sample trades can
 * be mapped with the reference-data stub running: ten different trade types, London and other entities, and one
 * counterparty (NONATURE) for which the stub has no nature, so the default {@code F} is used.
 */
@Component
@ConditionalOnProperty(name = "sftr.db.seed.enabled", havingValue = "true")
public class TradeSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(TradeSeeder.class);

    private static final String[] TRADE_TYPES = {"PO", "PI", "RP", "RV", "LP", "DP", "TP", "DV", "BP", "LC"};
    private static final String[] LEGAL_ENTITIES = {"LDNBB01", "BMOCM01", "LDN-02", "TORBB02", "LDNBB03",
            "BMOCM02", "LDNBB01", "BMOCM01", "LDN-02", "TORBB02"};

    private final EntityManager em;
    private final TransactionTemplate tx;
    private final SeedProperties props;

    public TradeSeeder(EntityManager em, TransactionTemplate tx, SeedProperties props) {
        this.em = em;
        this.tx = tx;
        this.props = props;
    }

    @Override
    public void run(ApplicationArguments args) {
        Long existing = tx.execute(s -> em.createQuery("select count(t) from Trade t", Long.class).getSingleResult());
        if (existing != null && existing > 0) {
            log.info("Seed skipped: the trade table already has {} rows", existing);
            return;
        }
        Instant now = Instant.now();
        tx.executeWithoutResult(s -> {
            for (int i = 1; i <= props.count(); i++) {
                em.persist(new Trade("seeder", now, TradeEvent.RECEIVED, json(i)));
            }
        });
        log.info("Seeded {} RECEIVED trades into the empty trade table", props.count());
    }

    /** Sample trade number {@code i} (1 for the first). The values rotate, so any count gives a mix. */
    static String json(int i) {
        String counterparty = i % 10 == 0 ? "NONATURE" : "CP" + (i % 10);
        return "{\"trade_ref\":\"SEED-" + i + "\",\"trade_type\":\"" + TRADE_TYPES[(i - 1) % TRADE_TYPES.length]
                + "\",\"bmoLegalEntity\":\"" + LEGAL_ENTITIES[(i - 1) % LEGAL_ENTITIES.length]
                + "\",\"counterpartyId\":\"" + counterparty + "\",\"group_cd\":\"G1\"}";
    }
}
