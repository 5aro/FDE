CREATE TABLE IF NOT EXISTS financial_statements (
    id BIGSERIAL PRIMARY KEY,

    corp_code VARCHAR(8) NOT NULL
        REFERENCES companies(corp_code),

    bsns_year INTEGER NOT NULL,

    reprt_code VARCHAR(5) NOT NULL,

    fs_div VARCHAR(3) NOT NULL,

    account_nm TEXT NOT NULL,

    account_id TEXT,

    thstrm_amount NUMERIC,

    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);