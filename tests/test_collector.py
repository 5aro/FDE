import sys
from pathlib import Path
from unittest.mock import Mock

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import collector


def test_get_company_success(monkeypatch):
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "status": "000",
        "corp_code": "00126380",
        "corp_name": "삼성전자(주)",
        "stock_code": "005930",
        "ceo_nm": "전영현, 노태문",
        "est_dt": "19690113",
    }

    mock_get = Mock(return_value=response)
    monkeypatch.setattr(collector.requests, "get", mock_get)

    company = collector.get_company("00126380")

    assert company["corp_name"] == "삼성전자(주)"
    assert mock_get.call_count == 1


def test_get_company_retry(monkeypatch):
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "status": "000",
        "corp_code": "00126380",
        "corp_name": "삼성전자(주)",
        "stock_code": "005930",
        "ceo_nm": "전영현, 노태문",
        "est_dt": "19690113",
    }

    mock_get = Mock(
        side_effect=[
            requests.exceptions.Timeout("Timeout"),
            response,
        ]
    )

    monkeypatch.setattr(collector.requests, "get", mock_get)
    monkeypatch.setattr(collector.time, "sleep", lambda _: None)

    company = collector.get_company("00126380")

    assert company is not None
    assert mock_get.call_count == 2


def test_get_company_unauthorized(monkeypatch):
    response = Mock()
    response.status_code = 401

    error = requests.exceptions.HTTPError(
        "401 Unauthorized",
        response=response,
    )

    mock_get = Mock(side_effect=error)
    monkeypatch.setattr(collector.requests, "get", mock_get)

    company = collector.get_company("00126380")

    assert company is None
    assert mock_get.call_count == 1
