import os
import psycopg
from dotenv import load_dotenv

load_dotenv()


def save_companies(companies):

    if not companies:
        print("저장할 기업정보가 없습니다.")
        return

    with psycopg.connect(
        dbname=os.getenv("DB_NAME", "fde"),
        user=os.getenv("DB_USER"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    ) as conn:
        with conn.cursor() as cur:
            for company in companies:
                cur.execute(
                    """
                    INSERT INTO companies (
                        corp_code,
                        corp_name,
                        stock_code,
                        ceo_nm,
                        est_dt
                    )
                    VALUES (
                        %(corp_code)s,
                        %(corp_name)s,
                        %(stock_code)s,
                        %(ceo_nm)s,
                        TO_DATE(%(est_dt)s, 'YYYYMMDD')
                    )
                    ON CONFLICT (corp_code)
                    DO UPDATE SET
                        corp_name = EXCLUDED.corp_name,
                        stock_code = EXCLUDED.stock_code,
                        ceo_nm = EXCLUDED.ceo_nm,
                        est_dt = EXCLUDED.est_dt
                    """,
                    company,
                )

    print(f"총 {len(companies)}개 기업 DB 저장 완료")
