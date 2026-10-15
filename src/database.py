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
    metrics = {
        "processed": len(companies),
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
    }

    if not companies:
        return metrics

    select_sql = """
        SELECT corp_code, corp_name, stock_code
        FROM companies
        WHERE corp_code = ANY(%s)
        FOR UPDATE
    """

    upsert_sql = """
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
        WHERE
            companies.corp_name IS DISTINCT FROM EXCLUDED.corp_name
            OR companies.stock_code IS DISTINCT FROM EXCLUDED.stock_code
    """

    with psycopg.connect(
        dbname=os.getenv("DB_NAME", "fde"),
        user=os.getenv("DB_USER"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        password=os.getenv("DB_PASSWORD"),
    ) as conn:

        with conn.cursor() as cur:
            corp_codes = [company["corp_code"] for company in companies]

            cur.execute(select_sql, (corp_codes,))

            existing = {
                row[0]: {
                    "corp_name": row[1],
                    "stock_code": row[2],
                }
                for row in cur.fetchall()
            }

            changed_companies = []

            for company in companies:
                corp_code = company["corp_code"]

                if corp_code not in existing:
                    metrics["inserted"] += 1
                    changed_companies.append(company)

                elif (
                    existing[corp_code]["corp_name"] != company["corp_name"]
                    or existing[corp_code]["stock_code"] != company["stock_code"]
                ):
                    metrics["updated"] += 1
                    changed_companies.append(company)

                else:
                    metrics["unchanged"] += 1

            if changed_companies:
                cur.executemany(upsert_sql, changed_companies)

    print(f"전체 처리: {metrics['processed']}건")
    print(f"신규 추가: {metrics['inserted']}건")
    print(f"정보 변경: {metrics['updated']}건")
    print(f"변경 없음: {metrics['unchanged']}건")

    return metrics


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


def finish_pipeline_run(
    run_id,
    status,
    records_processed=0,
    error_message=None,
    records_inserted=0,
    records_updated=0,
    records_unchanged=0,
):
    sql = """
        UPDATE pipeline_runs
        SET
            status = %s,
            finished_at = NOW(),
            records_processed = %s,
            error_message = %s,
            records_inserted = %s,
            records_updated = %s,
            records_unchanged = %s
        WHERE run_id = %s
    """

    with psycopg.connect(
        dbname=os.getenv("DB_NAME", "fde"),
        user=os.getenv("DB_USER"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        password=os.getenv("DB_PASSWORD"),
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    status,
                    records_processed,
                    error_message,
                    records_inserted,
                    records_updated,
                    records_unchanged,
                    run_id,
                ),
            )
