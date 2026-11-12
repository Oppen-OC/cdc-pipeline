-- Storage layer of the pipeline: the serving table the processor materialises
-- from the CDC stream and the API reads from.
--
-- It is kept in a separate schema from the source table so that the OLTP side
-- (public.products) and the analytical side stay clearly separated. In a real
-- deployment this would be a different database or cluster altogether; here a
-- schema keeps the exercise to a single PostgreSQL container.
--
-- The whole script is idempotent, so it can be re-run against a live database.

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.products_enriched (
    -- Same primary key as the source row: the processor UPSERTs on it, so the
    -- table always holds the current state of every product.
    id              INTEGER PRIMARY KEY,

    -- Fields carried over from the source table.
    name            VARCHAR(255)  NOT NULL,
    description     TEXT,
    price           NUMERIC(10,2) NOT NULL,

    -- Enrichment added by the processor.
    price_with_vat  NUMERIC(10,2) NOT NULL,
    category        VARCHAR(50),

    -- Debezium emits TIMESTAMP columns as microseconds since the epoch; the
    -- processor converts them before writing.
    created_at      TIMESTAMPTZ,

    -- CDC lineage.
    source_lsn      BIGINT        NOT NULL,
    last_op         CHAR(1)       NOT NULL,
    is_deleted      BOOLEAN       NOT NULL DEFAULT FALSE,
    ingested_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

COMMENT ON COLUMN analytics.products_enriched.source_lsn IS
    'Write-ahead log position of the change event that produced this row. The '
    'processor only applies an event whose LSN is greater than the stored one, '
    'which makes the at-least-once delivery of the Redis stream idempotent.';

COMMENT ON COLUMN analytics.products_enriched.last_op IS
    'Debezium operation code of the last applied event: c (create), '
    'r (snapshot read), u (update) or d (delete).';

COMMENT ON COLUMN analytics.products_enriched.is_deleted IS
    'Soft delete. A DELETE in the source table flags the row instead of '
    'removing it, so the analytical layer keeps the history.';

-- The API filters out deleted rows by default; a partial index keeps that
-- lookup cheap without indexing the deleted ones.
CREATE INDEX IF NOT EXISTS products_enriched_active_idx
    ON analytics.products_enriched (id)
    WHERE NOT is_deleted;

-- Supports the aggregation behind GET /stats.
CREATE INDEX IF NOT EXISTS products_enriched_category_idx
    ON analytics.products_enriched (category);
