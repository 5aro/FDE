from src.validator import validate_company


def test_valid_company():
    company = {
        "corp_code": "00126380",
        "corp_name": "삼성전자(주)",
        "stock_code": "005930",
        "ceo_nm": "전영현, 노태문",
        "est_dt": "19690113",
    }

    assert validate_company(company) is True


def test_invalid_corp_code():
    company = {
        "corp_code": "123",
        "corp_name": "삼성전자(주)",
        "stock_code": "005930",
        "ceo_nm": "전영현, 노태문",
        "est_dt": "19690113",
    }

    assert validate_company(company) is False


def test_invalid_date():
    company = {
        "corp_code": "00126380",
        "corp_name": "삼성전자(주)",
        "stock_code": "005930",
        "ceo_nm": "전영현, 노태문",
        "est_dt": "20261345",
    }

    assert validate_company(company) is False


def test_empty_company():
    assert validate_company(None) is False
