import os
from unittest.mock import patch

import psycopg
import pytest

from src.db_config import get_db_config
from src.financial_collector import (
    save_financial_statements,
    mark_collection_success,
)

TEST_DB = "fde_test"
TEST_CORP_CODE = "99999998"
TEST_YEAR = 2025


def test_financial_transaction_rollback():
    """재무제표 저장과 수집 상태 변경이 함께 롤백되는지 검증한다."""

    with patch.dict(os.environ, {"DB_NAME": TEST_DB}):
        db_config = get_db_config()

        assert db_config["dbname"] == TEST_DB

        # 테스트 데이터
        accounts = [
            {
                "corp_code": TEST_CORP_CODE,
                "bsns_year": str(TEST_YEAR),
                "reprt_code": "11011",
                "account_nm": "테스트매출액",
                "account_id": "test_Revenue",
                "thstrm_amount": "1000000",
                "sj_div": "IS",
                "account_detail": "-",
                "ord": "1",
                "currency": "KRW",
                "rcept_no": "99999999999999",
            }
        ]

        # 사전 조건: 테스트 기업과 RUNNING 상태를 준비한다.
        with psycopg.connect(**db_config) as conn:
            assert conn.info.dbname == TEST_DB

            conn.execute(
                """
                INSERT INTO companies (corp_code, corp_name)
                VALUES (%s, %s)
                ON CONFLICT (corp_code) DO NOTHING
                """,
                (TEST_CORP_CODE, "FDE 트랜잭션 테스트기업"),
            )

            conn.execute(
                """
                INSERT INTO financial_collection_status (
                    corp_code, bsns_year, fs_div, status
                )
                VALUES (%s, %s, %s, 'RUNNING')
                ON CONFLICT (corp_code, bsns_year, fs_div)
                DO UPDATE SET status = 'RUNNING'
                """,
                (TEST_CORP_CODE, str(TEST_YEAR), "CFS"),
            )

        try:
            # 실제 ETL 함수 통합 테스트
            with pytest.raises(RuntimeError, match="의도적인 테스트 오류"):
                with psycopg.connect(**db_config) as conn:
                    assert conn.info.dbname == TEST_DB

                    save_financial_statements(
                        accounts,
                        fs_div="CFS",
                        conn=conn,
                    )

                    mark_collection_success(
                        corp_code=TEST_CORP_CODE,
                        bsns_year=TEST_YEAR,
                        row_count=len(accounts),
                        conn=conn,
                        fs_div="CFS",
                    )

                    # COMMIT 이전에 의도적인 오류 발생
                    raise RuntimeError("의도적인 테스트 오류")

            # ROLLBACK 검증
            with psycopg.connect(**db_config) as conn:
                financial_count = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM financial_statements
                    WHERE corp_code = %s
                      AND bsns_year = %s
                      AND rcept_no = %s
                    """,
                    (TEST_CORP_CODE, TEST_YEAR, "99999999999999"),
                ).fetchone()[0]

                status = conn.execute(
                    """
                    SELECT status
                    FROM financial_collection_status
                    WHERE corp_code = %s
                      AND bsns_year = %s
                      AND fs_div = 'CFS'
                    """,
                    (TEST_CORP_CODE, str(TEST_YEAR)),
                ).fetchone()[0]

                assert financial_count == 0
                assert status == "RUNNING"

        finally:
            # 테스트 데이터 정리
            with psycopg.connect(**db_config) as conn:
                conn.execute(
                    "DELETE FROM financial_statements WHERE corp_code = %s",
                    (TEST_CORP_CODE,),
                )
                conn.execute(
                    "DELETE FROM financial_collection_status WHERE corp_code = %s",
                    (TEST_CORP_CODE,),
                )
                conn.execute(
                    "DELETE FROM companies WHERE corp_code = %s",
                    (TEST_CORP_CODE,),
                )
