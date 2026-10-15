import os
from unittest.mock import patch

import psycopg
from src.database import save_company_list

TEST_DB = "fde_test"
TEST_CORP_CODE = "99999999"


def test_incremental_loading_postgresql():
    company = {
        "corp_code": TEST_CORP_CODE,
        "corp_name": "FDE 테스트기업",
        "stock_code": "999999",
    }

    # 테스트용 데이터베이스 연결 설정
    with patch.dict(os.environ, {"DB_NAME": TEST_DB}):

        connection_params = {
            "dbname": TEST_DB,
            "user": os.getenv("DB_USER"),
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "password": os.getenv("DB_PASSWORD"),
        }

        # 실제 연결된 데이터베이스 확인
        with psycopg.connect(**connection_params) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                assert cur.fetchone()[0] == TEST_DB

                cur.execute(
                    "SELECT COUNT(*) FROM companies WHERE corp_code = %s",
                    (TEST_CORP_CODE,),
                )
                assert cur.fetchone()[0] == 0, "테스트 기업이 이미 존재합니다."

        try:
            # 1. 신규 INSERT
            first = save_company_list([company])

            assert first == {
                "processed": 1,
                "inserted": 1,
                "updated": 0,
                "unchanged": 0,
            }

            # 2. 동일 데이터 SKIP
            second = save_company_list([company])

            assert second == {
                "processed": 1,
                "inserted": 0,
                "updated": 0,
                "unchanged": 1,
            }

            # 3. 기존 데이터 UPDATE
            updated_company = {
                **company,
                "corp_name": "FDE 변경기업",
            }

            third = save_company_list([updated_company])

            assert third == {
                "processed": 1,
                "inserted": 0,
                "updated": 1,
                "unchanged": 0,
            }

            # 4. 실제 DB에 변경 내용이 반영됐는지 확인
            with psycopg.connect(**connection_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT corp_name
                        FROM companies
                        WHERE corp_code = %s
                        """,
                        (TEST_CORP_CODE,),
                    )

                    assert cur.fetchone()[0] == "FDE 변경기업"

        finally:
            # 테스트 성공/실패 여부와 관계없이 데이터 삭제
            with psycopg.connect(**connection_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM companies WHERE corp_code = %s",
                        (TEST_CORP_CODE,),
                    )
