CREATE TABLE IF NOT EXISTS companies (
    corp_code VARCHAR(8) PRIMARY KEY,
    corp_name TEXT NOT NULL,
    stock_code VARCHAR(6),
    ceo_nm TEXT,
    est_dt DATE
);