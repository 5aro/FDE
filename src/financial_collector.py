"""
OpenDART 재무제표 수집기

기능:
1. OpenDART 재무제표 API 호출
2. 재무제표 금액 데이터 변환
3. PostgreSQL 저장
4. UPSERT를 통한 중복 데이터 방지
"""

import os
from decimal import Decimal, InvalidOperation
from contextlib import nullcontext

import psycopg
import requests

if __package__:
    from .db_config import get_db_config
else:
    from db_config import get_db_config


def fetch_financial_statements(
    corp_code: str,
    bsns_year: int,
    reprt_code: str = "11011",
    fs_div: str = "CFS",
):
    """OpenDART API에서 기업 재무제표를 수집한다."""

    api_key = os.getenv("DART_API_KEY")

    if not api_key:
        raise ValueError("DART_API_KEY 환경변수가 없습니다.")

    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"

    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": bsns_year,
        "reprt_code": reprt_code,
        "fs_div": fs_div,
    }

    response = requests.get(
        url,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    data = response.json()

    if data.get("status") != "000":
        raise RuntimeError(
            f"OpenDART API 오류: " f"{data.get('status')} - {data.get('message')}"
        )

    return data


def parse_amount(value):
    """재무제표 금액 문자열을 Decimal로 변환한다."""

    if value is None:
        return None

    value = str(value).strip().replace(",", "")

    if value in ("", "-"):
        return None

    try:
        return Decimal(value)

    except InvalidOperation:
        raise ValueError(f"잘못된 금액 형식: {value}")


def save_financial_statements(accounts: list, fs_div: str = "CFS", conn=None):
    """
    재무제표 데이터를 PostgreSQL에 저장한다.

    Args:
        accounts: OpenDART API에서 반환된 계정 목록
        fs_div: 연결(CFS) 또는 별도(OFS) 재무제표

    동작:
        - 신규 계정 INSERT
        - 기존 계정 UPDATE
        - 동일 계정 중복 저장 방지
    """

    if not accounts:
        raise ValueError("저장할 재무제표 데이터가 없습니다.")

    db_config = get_db_config()
    db_name = db_config["dbname"]

    rows = []

    for account in accounts:
        rows.append(
            (
                account["corp_code"],
                int(account["bsns_year"]),
                account["reprt_code"],
                fs_div,
                account["account_nm"],
                account.get("account_id"),
                parse_amount(account.get("thstrm_amount")),
                account["sj_div"],
                account.get("account_detail"),
                int(account["ord"]),
                account.get("currency"),
                account["rcept_no"],
            )
        )

    connection_context = (
        nullcontext(conn) if conn is not None else psycopg.connect(**db_config)
    )

    with connection_context as active_conn:
        with active_conn.cursor() as cur:
            # 기존 검증 및 UPSERT 로직 그대로 유지

            # 실제 연결된 DB 확인
            cur.execute("SELECT current_database()")

            actual_db = cur.fetchone()[0]

            if actual_db != db_name:
                raise RuntimeError(
                    f"DB 연결 불일치: " f"예상={db_name}, 실제={actual_db}"
                )

            # 재무제표에 포함된 기업이 companies에 존재하는지 확인
            corp_codes = sorted({row[0] for row in rows})

            cur.execute(
                """
                    SELECT corp_code
                    FROM companies
                    WHERE corp_code = ANY(%s)
                    """,
                (corp_codes,),
            )

            existing_codes = {row[0] for row in cur.fetchall()}

            missing_codes = set(corp_codes) - existing_codes

            if missing_codes:
                raise ValueError(
                    f"기업정보가 존재하지 않습니다: " f"{sorted(missing_codes)}"
                )

            # 재무제표 UPSERT
            cur.executemany(
                """
                    INSERT INTO financial_statements (
                        corp_code,
                        bsns_year,
                        reprt_code,
                        fs_div,
                        account_nm,
                        account_id,
                        thstrm_amount,
                        sj_div,
                        account_detail,
                        ord,
                        currency,
                        rcept_no
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT ON CONSTRAINT
                        uq_financial_statement_row
                    DO UPDATE SET
                        account_nm = EXCLUDED.account_nm,
                        account_id = EXCLUDED.account_id,
                        thstrm_amount = EXCLUDED.thstrm_amount,
                        currency = EXCLUDED.currency,
                        collected_at = NOW()
                    """,
                rows,
            )

        print(
            f"재무제표 저장 완료: "
            f"기업 {len(corp_codes)}개, "
            f"계정 {len(rows)}개, "
            f"DB={db_name}"
        )


def get_target_companies():
    """
    PostgreSQL에서 수집 대상 기업 5개의 고유번호를 조회한다.

    종목코드 기준으로 조회하므로
    기업 고유번호를 코드에 직접 입력할 필요가 없다.
    """
    stock_codes = [
        "005930",  # 삼성전자
        "000660",  # SK하이닉스
        "005380",  # 현대자동차
        "035420",  # NAVER
        "066570",  # LG전자
    ]

    with psycopg.connect(
        dbname=os.getenv("DB_NAME", "fde_test"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT corp_code, corp_name
                FROM companies
                WHERE stock_code = ANY(%s)
                ORDER BY corp_name
                """,
                (stock_codes,),
            )

            companies = cur.fetchall()

    if len(companies) != 5:
        raise ValueError(f"수집 대상 기업 수가 올바르지 않습니다: {len(companies)}개")

    return companies


def collect_target_companies_financials(
    bsns_year: int = 2025,
):
    """기업별 재무제표를 증분 수집하고 상태를 관리한다."""

    companies = get_target_companies()

    success = 0
    skipped = 0
    failures = []

    for corp_code, corp_name in companies:

        # 1. 기존 수집 상태 확인
        status = get_collection_status(
            corp_code=corp_code,
            bsns_year=bsns_year,
            fs_div="CFS",
        )

        if status == "SUCCESS":
            print(f"[SKIP] {corp_name}: 수집 완료")
            skipped += 1
            continue

        if status == "RUNNING":
            raise RuntimeError(
                f"{corp_name}: 이미 RUNNING 상태입니다. "
                "기존 작업이 종료되었는지 확인하세요."
            )

        try:
            print(f"[START] {corp_name} ({corp_code})")

            # 2. 수집 시작 기록
            mark_collection_running(
                corp_code=corp_code,
                bsns_year=bsns_year,
                fs_div="CFS",
            )

            # 3. OpenDART API 호출
            result = fetch_financial_statements(
                corp_code=corp_code,
                bsns_year=bsns_year,
                reprt_code="11011",
                fs_div="CFS",
            )

            accounts = result.get("list", [])

            if not accounts:
                raise ValueError("재무제표 데이터가 없습니다.")

            # 4. 재무제표 저장 + SUCCESS 기록
            # 동일한 트랜잭션에서 실행
            with psycopg.connect(**get_db_config()) as conn:

                save_financial_statements(
                    accounts,
                    fs_div="CFS",
                    conn=conn,
                )

                mark_collection_success(
                    corp_code=corp_code,
                    bsns_year=bsns_year,
                    row_count=len(accounts),
                    conn=conn,
                    fs_div="CFS",
                )

            success += 1

            print(f"[SUCCESS] {corp_name}: " f"{len(accounts)}건")

        except Exception as exc:
            failures.append((corp_name, str(exc)))

            # 5. 실패 상태 기록
            try:
                mark_collection_failed(
                    corp_code=corp_code,
                    bsns_year=bsns_year,
                    error_message=str(exc),
                    fs_div="CFS",
                )
            except Exception as status_exc:
                print(
                    f"[WARNING] {corp_name}: " f"FAILED 상태 기록 실패 - {status_exc}"
                )

            print(f"[FAILED] {corp_name}: {exc}")

    print(
        f"수집 종료: 성공 {success}개 / " f"생략 {skipped}개 / 실패 {len(failures)}개"
    )

    if failures:
        raise RuntimeError(f"재무제표 수집 실패 기업: {failures}")


def is_financial_data_collected(
    corp_code: str,
    bsns_year: int,
    fs_div: str = "CFS",
) -> bool:
    """
    특정 기업의 재무제표가 이미 DB에 저장되어 있는지 확인한다.

    True  : 기존 데이터 존재
    False : 기존 데이터 없음
    """
    with psycopg.connect(
        dbname=os.getenv("DB_NAME", "fde_test"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM financial_statements
                    WHERE corp_code = %s
                      AND bsns_year = %s
                      AND fs_div = %s
                )
                """,
                (corp_code, bsns_year, fs_div),
            )

            return cur.fetchone()[0]


def get_collection_status(
    corp_code: str,
    bsns_year: int,
    fs_div: str = "CFS",
) -> str | None:
    """기업별 재무제표 수집 상태를 조회한다."""

    db_config = get_db_config()

    with psycopg.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status
                FROM financial_collection_status
                WHERE corp_code = %s
                  AND bsns_year = %s
                  AND fs_div = %s
                """,
                (corp_code, str(bsns_year), fs_div),
            )

            result = cur.fetchone()

    return result[0] if result else None


def mark_collection_running(
    corp_code: str,
    bsns_year: int,
    fs_div: str = "CFS",
):
    """재무제표 수집 시작 상태를 기록한다."""

    db_config = get_db_config()

    with psycopg.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO financial_collection_status (
                    corp_code,
                    bsns_year,
                    fs_div,
                    status,
                    started_at,
                    completed_at,
                    error_message
                )
                VALUES (
                    %s, %s, %s,
                    'RUNNING', NOW(), NULL, NULL
                )
                ON CONFLICT (corp_code, bsns_year, fs_div)
                DO UPDATE SET
                    status = 'RUNNING',
                    started_at = NOW(),
                    completed_at = NULL,
                    error_message = NULL,
                    retry_count =
                        financial_collection_status.retry_count + 1
                """,
                (corp_code, str(bsns_year), fs_div),
            )


def mark_collection_failed(
    corp_code: str,
    bsns_year: int,
    error_message: str,
    fs_div: str = "CFS",
):
    """재무제표 수집 실패 상태를 기록한다."""

    db_config = get_db_config()

    with psycopg.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE financial_collection_status
                SET
                    status = 'FAILED',
                    completed_at = NOW(),
                    error_message = %s
                WHERE corp_code = %s
                  AND bsns_year = %s
                  AND fs_div = %s
                  AND status = 'RUNNING'
                """,
                (
                    error_message,
                    corp_code,
                    str(bsns_year),
                    fs_div,
                ),
            )

            if cur.rowcount != 1:
                raise RuntimeError("RUNNING 상태의 수집 작업을 찾을 수 없습니다.")


def mark_collection_success(
    corp_code: str,
    bsns_year: int,
    row_count: int,
    conn,
    fs_div: str = "CFS",
):
    """재무제표 수집 성공 상태를 기록한다."""

    if row_count <= 0:
        raise ValueError("저장된 재무제표 계정 수가 올바르지 않습니다.")

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE financial_collection_status
            SET
                status = 'SUCCESS',
                row_count = %s,
                completed_at = NOW(),
                error_message = NULL
            WHERE corp_code = %s
              AND bsns_year = %s
              AND fs_div = %s
              AND status = 'RUNNING'
            """,
            (
                row_count,
                corp_code,
                str(bsns_year),
                fs_div,
            ),
        )

        if cur.rowcount != 1:
            raise RuntimeError("RUNNING 상태의 수집 작업을 찾을 수 없습니다.")


if __name__ == "__main__":
    corp_code = "00126380"
    bsns_year = 2025

    mark_collection_running(
        corp_code=corp_code,
        bsns_year=bsns_year,
    )

    mark_collection_failed(
        corp_code=corp_code,
        bsns_year=bsns_year,
        error_message="테스트용 API 호출 실패",
    )

    print(
        get_collection_status(
            corp_code=corp_code,
            bsns_year=bsns_year,
        )
    )
