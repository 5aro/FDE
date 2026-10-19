SELECT
    id,
    corp_code,
    bsns_year,
    reprt_code,
    fs_div,
    sj_div,
    account_id,
    account_nm,
    account_detail,
    thstrm_amount,
    currency,
    rcept_no,
    ord,
    collected_at
FROM {{ source('fde', 'financial_statements') }}
WHERE corp_code IS NOT NULL
  AND bsns_year IS NOT NULL
