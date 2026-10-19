{{ config(materialized='table') }}
WITH financial_accounts AS (
    SELECT
        corp_code,
        bsns_year,
        reprt_code,
        fs_div,
        sj_div,
        account_id,
        thstrm_amount,
        ROW_NUMBER() OVER (
            PARTITION BY
                corp_code,
                bsns_year,
                reprt_code,
                fs_div,
                account_id
            ORDER BY
                CASE sj_div
                    WHEN 'IS' THEN 1
                    WHEN 'CIS' THEN 2
                    ELSE 3
                END,
                ord,
                id
        ) AS rn
    FROM {{ ref('stg_financial_statements') }}
    WHERE sj_div IN ('IS', 'CIS')
      AND account_id IN (
          'ifrs-full_Revenue',
          'dart_OperatingIncomeLoss',
          'ifrs-full_ProfitLoss'
      )
),

financial_summary AS (
    SELECT
        corp_code,
        bsns_year,
        reprt_code,
        fs_div,

        MAX(
            CASE
                WHEN account_id = 'ifrs-full_Revenue'
                THEN thstrm_amount
            END
        ) AS revenue,

        MAX(
            CASE
                WHEN account_id = 'dart_OperatingIncomeLoss'
                THEN thstrm_amount
            END
        ) AS operating_income,

        MAX(
            CASE
                WHEN account_id = 'ifrs-full_ProfitLoss'
                THEN thstrm_amount
            END
        ) AS net_income

    FROM financial_accounts
    WHERE rn = 1
    GROUP BY
        corp_code,
        bsns_year,
        reprt_code,
        fs_div
)

SELECT
    c.corp_code,
    c.corp_name,
    f.bsns_year,
    f.reprt_code,
    f.fs_div,
    f.revenue,
    f.operating_income,
    f.net_income,

    ROUND(
        f.operating_income::numeric
        / NULLIF(f.revenue::numeric, 0) * 100,
        2
    ) AS operating_margin,

    ROUND(
        f.net_income::numeric
        / NULLIF(f.revenue::numeric, 0) * 100,
        2
    ) AS net_margin

FROM financial_summary f

LEFT JOIN {{ ref('stg_companies') }} c
    ON f.corp_code = c.corp_code
