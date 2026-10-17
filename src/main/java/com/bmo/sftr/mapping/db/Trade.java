package com.bmo.sftr.mapping.db;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * One row of the {@code trade} table, which another application fills. This application only reads it.
 * {@code data_json} holds the trade as JSON; it is read as text and handed to the pipeline unchanged.
 * Change the column names here if the real table differs.
 */
@Entity
@Immutable
@Table(name = "trade")
public class Trade {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)       // the database assigns the id (serial or identity column)
    private Long id;

    @Column(name = "created_by")
    private String createdBy;

    @Column(name = "created_on")
    private Instant createdOn;

    @Enumerated(EnumType.STRING)
    @Column(name = "event")
    private TradeEvent event;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "data_json")
    private String dataJson;

    protected Trade() {
        // for JPA
    }

    /** A new row; the database assigns the id when it is saved. */
    public Trade(String createdBy, Instant createdOn, TradeEvent event, String dataJson) {
        this(null, createdBy, createdOn, event, dataJson);
    }

    /** A row with a known id, for tests that build rows by hand. */
    public Trade(Long id, String createdBy, Instant createdOn, TradeEvent event, String dataJson) {
        this.id = id;
        this.createdBy = createdBy;
        this.createdOn = createdOn;
        this.event = event;
        this.dataJson = dataJson;
    }

    public Long getId() { return id; }
    public String getCreatedBy() { return createdBy; }
    public Instant getCreatedOn() { return createdOn; }
    public TradeEvent getEvent() { return event; }
    public String getDataJson() { return dataJson; }
}
