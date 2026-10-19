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


def test_financial_transaction_commit():
    """재무제표 저장과 수집 상태 변경이 함께 COMMIT되는지 검증한다."""

    test_corp_code = "99999997"
    test_year = 2025
    test_rcept_no = "99999999999997"

    accounts = [
        {
            "corp_code": test_corp_code,
            "bsns_year": str(test_year),
            "reprt_code": "11011",
            "account_nm": "테스트매출액",
            "account_id": "test_Revenue",
            "thstrm_amount": "1000000",
            "sj_div": "IS",
            "account_detail": "-",
            "ord": "1",
            "currency": "KRW",
            "rcept_no": test_rcept_no,
        }
    ]

    with patch.dict(os.environ, {"DB_NAME": "fde_test"}):
        db_config = get_db_config()

        assert db_config["dbname"] == "fde_test"

        with psycopg.connect(**db_config) as conn:
            assert conn.info.dbname == "fde_test"

            # 기존 테스트 데이터와 충돌하지 않는지 확인
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM companies WHERE corp_code = %s",
                    (test_corp_code,),
                ).fetchone()[0]
                == 0
            ), "테스트 기업 코드가 이미 사용 중입니다."

            # 테스트 기업 생성
            conn.execute(
                """
                INSERT INTO companies (corp_code, corp_name)
                VALUES (%s, %s)
                """,
                (test_corp_code, "FDE COMMIT 테스트기업"),
            )

            # 수집 상태 생성
            conn.execute(
                """
                INSERT INTO financial_collection_status (
                    corp_code, bsns_year, fs_div, status
                )
                VALUES (%s, %s, %s, 'RUNNING')
                """,
                (test_corp_code, str(test_year), "CFS"),
            )

        try:
            # 실제 ETL 함수 실행
            with psycopg.connect(**db_config) as conn:
                save_financial_statements(
                    accounts,
                    fs_div="CFS",
                    conn=conn,
                )

                mark_collection_success(
                    corp_code=test_corp_code,
                    bsns_year=test_year,
                    row_count=len(accounts),
                    conn=conn,
                    fs_div="CFS",
                )

            # 트랜잭션 COMMIT 이후 검증
            with psycopg.connect(**db_config) as conn:
                financial_count = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM financial_statements
                    WHERE corp_code = %s
                      AND bsns_year = %s
                      AND rcept_no = %s
                    """,
                    (test_corp_code, test_year, test_rcept_no),
                ).fetchone()[0]

                status_row = conn.execute(
                    """
                    SELECT status, row_count
                    FROM financial_collection_status
                    WHERE corp_code = %s
                      AND bsns_year = %s
                      AND fs_div = 'CFS'
                    """,
                    (test_corp_code, str(test_year)),
                ).fetchone()

                assert financial_count == 1
                assert status_row == ("SUCCESS", 1)

        finally:
            # 테스트 데이터 정리
            with psycopg.connect(**db_config) as conn:
                conn.execute(
                    "DELETE FROM financial_statements WHERE corp_code = %s",
                    (test_corp_code,),
                )
                conn.execute(
                    "DELETE FROM financial_collection_status WHERE corp_code = %s",
                    (test_corp_code,),
                )
                conn.execute(
                    "DELETE FROM companies WHERE corp_code = %s",
                    (test_corp_code,),
                )


def test_skip_already_collected_financials():
    """이미 수집 완료된 기업은 API 호출 없이 건너뛴다."""

    from unittest.mock import patch

    from src.financial_collector import collect_target_companies_financials

    companies = [
        ("00126380", "삼성전자"),
        ("00164779", "테스트기업"),
    ]

    with (
        patch(
            "src.financial_collector.get_target_companies",
            return_value=companies,
        ),
        patch(
            "src.financial_collector.get_collection_status",
            return_value="SUCCESS",
        ) as mock_status,
        patch("src.financial_collector.fetch_financial_statements") as mock_fetch,
        patch("src.financial_collector.save_financial_statements") as mock_save,
        patch("src.financial_collector.mark_collection_running") as mock_running,
    ):
        collect_target_companies_financials(bsns_year=2025)

    # 두 기업의 수집 상태를 확인했는지 검증
    assert mock_status.call_count == 2

    # 이미 수집한 기업은 API를 다시 호출하지 않음
    mock_fetch.assert_not_called()

    # DB 저장 함수도 실행하지 않음
    mock_save.assert_not_called()

    # RUNNING 상태로 변경하지 않음
    mock_running.assert_not_called()


def test_retry_failed_financial_collection():
    """FAILED 상태의 기업은 재수집을 시도한다."""

    from unittest.mock import patch

    from src.financial_collector import (
        collect_target_companies_financials,
    )

    companies = [
        ("00126380", "삼성전자"),
    ]

    accounts = [
        {"account_nm": "매출액"},
    ]

    with (
        patch(
            "src.financial_collector.get_target_companies",
            return_value=companies,
        ),
        patch(
            "src.financial_collector.get_collection_status",
            return_value="FAILED",
        ) as mock_status,
        patch(
            "src.financial_collector.mark_collection_running",
        ) as mock_running,
        patch(
            "src.financial_collector.fetch_financial_statements",
            return_value={"list": accounts},
        ) as mock_fetch,
        patch(
            "src.financial_collector.psycopg.connect",
        ) as mock_connect,
        patch(
            "src.financial_collector.save_financial_statements",
        ) as mock_save,
        patch(
            "src.financial_collector.mark_collection_success",
        ) as mock_success,
    ):
        collect_target_companies_financials(bsns_year=2025)

    mock_status.assert_called_once_with(
        corp_code="00126380",
        bsns_year=2025,
        fs_div="CFS",
    )

    mock_running.assert_called_once()

    mock_fetch.assert_called_once()

    mock_connect.assert_called_once()

    mock_save.assert_called_once()

    mock_success.assert_called_once()
