-- QCDCF Watermark Table
-- This table is used by the CDC framework to write low and high watermark
-- boundaries during snapshot chunk reads. These markers appear in the WAL
-- and are used by the reconciliation engine to detect the window boundaries.

CREATE TABLE IF NOT EXISTS qcdcf_watermark (
    id           BIGSERIAL PRIMARY KEY,
    window_id    UUID NOT NULL,
    boundary     VARCHAR(4) NOT NULL CHECK (boundary IN ('LOW', 'HIGH')),
    table_schema VARCHAR(255) NOT NULL,
    table_name   VARCHAR(255) NOT NULL,
    chunk_index  INTEGER NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qcdcf_watermark_window
    ON qcdcf_watermark (window_id);
