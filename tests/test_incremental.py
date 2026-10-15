from unittest.mock import MagicMock, patch

from src.database import save_company_list


def test_incremental_loading():
    companies = [
        {
            "corp_code": "00000001",
            "corp_name": "신규기업",
            "stock_code": "000001",
        },
        {
            "corp_code": "00000002",
            "corp_name": "변경기업",
            "stock_code": "000002",
        },
        {
            "corp_code": "00000003",
            "corp_name": "동일기업",
            "stock_code": "000003",
        },
    ]

    # DB에 이미 존재하는 기업
    existing_rows = [
        ("00000002", "이전기업명", "000002"),
        ("00000003", "동일기업", "000003"),
    ]

    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = existing_rows

    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("src.database.psycopg.connect", return_value=mock_conn):
        metrics = save_company_list(companies)

    assert metrics == {
        "processed": 3,
        "inserted": 1,
        "updated": 1,
        "unchanged": 1,
    }

    # 신규 및 변경 기업만 적재했는지 확인
    args, _ = mock_cursor.executemany.call_args
    assert len(args[1]) == 2
