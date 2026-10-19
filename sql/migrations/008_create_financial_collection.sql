CREATE TABLE IF NOT EXISTS financial_collection_status (
    corp_code VARCHAR(8) NOT NULL,
    bsns_year VARCHAR(4) NOT NULL,
    fs_div VARCHAR(3) NOT NULL,

    status VARCHAR(20) NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,

    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,

    PRIMARY KEY (corp_code, bsns_year, fs_div),

    CONSTRAINT chk_collection_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),

    CONSTRAINT chk_collection_row_count
        CHECK (row_count >= 0),

    CONSTRAINT chk_collection_retry_count
        CHECK (retry_count >= 0)
);