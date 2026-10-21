"""
FDE Airflow ETL Pipeline

실행 순서:
1. 기업정보 수집
2. 기업정보 존재 여부 검증
3. 기업정보 품질 검사
4. 재무제표 수집 및 PostgreSQL 저장
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="fde_daily_pipeline",
    schedule="0 9 * * *",
    start_date=datetime(2026, 10, 14),
    catchup=False,
    tags=["FDE", "OpenDART", "ETL"],
)
def fde_pipeline():

    # -----------------------------------------
    # TASK 1. 기업정보 수집
    # -----------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def collect_corporations():

        import sys

        sys.path.insert(0, "/opt/airflow/src")

        from corp_list import main

        main()

    # -----------------------------------------
    # TASK 2. 기업정보 검증
    # -----------------------------------------
    @task()
    def validate_results():

        import sys

        import psycopg

        sys.path.insert(0, "/opt/airflow/src")
        from db_config import get_db_config

        with psycopg.connect(**get_db_config()) as conn, conn.cursor() as cur:

            cur.execute("SELECT COUNT(*) FROM companies")

            count = cur.fetchone()[0]

        if count == 0:
            raise ValueError("기업정보가 존재하지 않습니다.")

        print(f"기업정보 검증 완료: {count}건")

    # -----------------------------------------
    # TASK 3. 데이터 품질 검사
    # -----------------------------------------
    @task()
    def check_data_quality():

        import sys

        import psycopg

        sys.path.insert(0, "/opt/airflow/src")
        from db_config import get_db_config

        with psycopg.connect(**get_db_config()) as conn, conn.cursor() as cur:

            cur.execute("""
                    SELECT
                        COUNT(*) AS total_rows,
                        COUNT(DISTINCT corp_code)
                            AS unique_companies,
                        COUNT(*) FILTER (
                            WHERE corp_name IS NULL
                               OR BTRIM(corp_name) = ''
                        ) AS invalid_names,
                        COUNT(*) FILTER (
                            WHERE stock_code IS NULL
                               OR stock_code !~ '^[0-9A-Z]{6}$'
                        ) AS invalid_stock_codes
                    FROM companies
                    """)

            (
                total_rows,
                unique_companies,
                invalid_names,
                invalid_stock_codes,
            ) = cur.fetchone()

        print(f"전체 기업 수: {total_rows}")
        print(f"고유 기업 수: {unique_companies}")
        print(f"기업명 오류: {invalid_names}")
        print(f"주식코드 형식 오류: {invalid_stock_codes}")

        errors = []

        if total_rows == 0:
            errors.append("기업 데이터가 존재하지 않습니다.")

        if total_rows != unique_companies:
            errors.append("기업 코드 중복이 발견됐습니다.")

        if invalid_names > 0:
            errors.append(f"기업명 오류: {invalid_names}건")

        if invalid_stock_codes > 0:
            errors.append(f"주식코드 형식 오류: {invalid_stock_codes}건")

        if errors:
            raise ValueError("데이터 품질 검증 실패: " + "; ".join(errors))

        print("데이터 품질 검증 SUCCESS")

    # -----------------------------------------
    # TASK 4. 재무제표 수집
    # -----------------------------------------
    @task(
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
    def collect_financial_statements():
        import os
        import sys

        # Airflow 컨테이너에서 Mac PostgreSQL로 접속
        os.environ["DB_HOST"] = os.environ["FDE_DB_HOST"]
        os.environ["DB_PORT"] = os.environ["FDE_DB_PORT"]
        os.environ["DB_NAME"] = os.environ["FDE_DB_NAME"]
        os.environ["DB_USER"] = os.environ["FDE_DB_USER"]
        os.environ["DB_PASSWORD"] = os.environ["FDE_DB_PASSWORD"]

        sys.path.insert(0, "/opt/airflow/src")

        from financial_collector import (
            collect_target_companies_financials,
        )

        collect_target_companies_financials(
            bsns_year=2025,
        )

    # -----------------------------------------
    # TASK 실행 순서
    # -----------------------------------------
    (
        collect_corporations()
        >> validate_results()
        >> check_data_quality()
        >> collect_financial_statements()
    )


fde_pipeline()
