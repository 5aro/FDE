WITH financial_summary AS (
    SELECT
        corp_code,

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

    FROM financial_statements

    WHERE bsns_year = 2025
      AND fs_div = 'CFS'
      AND sj_div IN ('IS', 'CIS')

    GROUP BY corp_code
)

SELECT
    c.corp_name AS 기업명,

    ROUND(f.revenue / 1e12, 2) AS 매출액_조원,

    ROUND(f.operating_income / 1e12, 2) AS 영업이익_조원,

    ROUND(f.net_income / 1e12, 2) AS 당기순이익_조원,

    ROUND(
        f.operating_income * 100.0
        / NULLIF(f.revenue, 0),
        2
    ) AS 영업이익률

FROM financial_summary f

JOIN companies c
    ON f.corp_code = c.corp_code

ORDER BY f.revenue DESC;