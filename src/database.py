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


def save_company_list(companies):
    if not companies:
        print("저장할 기업 목록이 없습니다.")
        return

    sql = """
        INSERT INTO companies (
            corp_code,
            corp_name,
            stock_code
        )
        VALUES (
            %(corp_code)s,
            %(corp_name)s,
            %(stock_code)s
        )
        ON CONFLICT (corp_code)
        DO UPDATE SET
            corp_name = EXCLUDED.corp_name,
            stock_code = EXCLUDED.stock_code
    """

    with psycopg.connect(
        dbname=os.getenv("DB_NAME", "fde"),
        user=os.getenv("DB_USER"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    ) as conn:

        with conn.cursor() as cur:
            cur.executemany(sql, companies)

    print(f"기업 목록 {len(companies)}건 DB 적재 완료")


def start_pipeline_run(pipeline_name):
    sql = """
        INSERT INTO pipeline_runs (pipeline_name, status)
        VALUES (%s, 'RUNNING')
        RETURNING run_id
    """

    with psycopg.connect(
        dbname=os.getenv("DB_NAME", "fde"),
        user=os.getenv("DB_USER"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (pipeline_name,))
            run_id = cur.fetchone()[0]

    return run_id


def finish_pipeline_run(run_id, status, records_processed=0, error_message=None):
    sql = """
        UPDATE pipeline_runs
        SET
            status = %s,
            finished_at = NOW(),
            records_processed = %s,
            error_message = %s
        WHERE run_id = %s
    """

    with psycopg.connect(
        dbname=os.getenv("DB_NAME", "fde"),
        user=os.getenv("DB_USER"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (status, records_processed, error_message, run_id),
            )
