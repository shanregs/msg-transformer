-- The trade table and a few sample rows, for LOCAL development only.
-- In the real system another application owns this table and fills it; this application only reads it.
-- Run it against the sftr database:
--
--   psql -U postgres -h localhost -d sftr -f samples/db/02-trade-table.sql
--
-- Column names are mapped in src/main/java/com/bmo/sftr/mapping/db/Trade.java; change both if the real table differs.

CREATE TABLE IF NOT EXISTS trade (
    id          BIGSERIAL    PRIMARY KEY,
    created_by  VARCHAR(100) NOT NULL,
    created_on  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    event       VARCHAR(20)  NOT NULL CHECK (event IN ('RECEIVED', 'PROCESSED', 'PUBLISHED')),
    data_json   JSONB        NOT NULL
);

-- the application reads RECEIVED rows in id order, a page at a time
CREATE INDEX IF NOT EXISTS ix_trade_event_id ON trade (event, id);

-- Sample rows. With the reference-data stub running (python samples/refdata-stub.py), a run gives:
--   rows 1, 2, 3 -> written          (row 1: London entity, GIVE; row 2: not London, TAKE; row 3: nature falls back to F)
--   row 4        -> failed           (reference data answers HTTP 400)
--   row 5        -> failed           (valid JSON but not an object)
--   rows 6, 7    -> not read         (their event is not RECEIVED)
INSERT INTO trade (created_by, event, data_json) VALUES
    ('other-app', 'RECEIVED',  '{"trade_ref":"T1","trade_type":"PO","bmoLegalEntity":"LDNBB01","counterpartyId":"CP1","group_cd":"G1"}'),
    ('other-app', 'RECEIVED',  '{"trade_ref":"T2","trade_type":"PI","bmoLegalEntity":"BMOCM01","counterpartyId":"CP2","group_cd":"G1"}'),
    ('other-app', 'RECEIVED',  '{"trade_ref":"T3","trade_type":"PO","bmoLegalEntity":"LDNBB01","counterpartyId":"NONATURE","group_cd":"G1"}'),
    ('other-app', 'RECEIVED',  '{"trade_ref":"T4","trade_type":"PO","bmoLegalEntity":"LDNBB01","counterpartyId":"FAIL4XX"}'),
    ('other-app', 'RECEIVED',  '[1, 2, 3]'),
    ('other-app', 'PROCESSED', '{"trade_ref":"T6","trade_type":"PO","bmoLegalEntity":"LDNBB01","counterpartyId":"CP1"}'),
    ('other-app', 'PUBLISHED', '{"trade_ref":"T7","trade_type":"PI","bmoLegalEntity":"BMOCM01","counterpartyId":"CP2"}');
