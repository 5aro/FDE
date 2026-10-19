SELECT
    corp_code,
    bsns_year,
    reprt_code,
    fs_div,
    COUNT(*) AS duplicate_count
FROM {{ ref('mart_financial_summary') }}
GROUP BY
    corp_code,
    bsns_year,
    reprt_code,
    fs_div
HAVING COUNT(*) > 1
