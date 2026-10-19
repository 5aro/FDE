SELECT
    corp_code,
    corp_name,
    stock_code,
    ceo_nm,
    est_dt
FROM {{ source('fde', 'companies') }}
WHERE corp_code IS NOT NULL
