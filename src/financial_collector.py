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


def save_financial_statements(
    accounts: list,
    fs_div: str = "CFS",
):
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

    with psycopg.connect(**db_config) as conn:

        with conn.cursor() as cur:

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


if __name__ == "__main__":

    result = fetch_financial_statements(
        corp_code="00126380",
        bsns_year=2025,
        reprt_code="11011",
        fs_div="CFS",
    )

    accounts = result["list"]

    print(f"수집 계정 수: {len(accounts)}")

    save_financial_statements(
        accounts,
        fs_div="CFS",
    )
