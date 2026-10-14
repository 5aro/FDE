import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import corp_list


def test_pipeline_success(monkeypatch):
    companies = [
        {
            "corp_code": "00126380",
            "corp_name": "삼성전자",
            "stock_code": "005930",
        }
    ]

    mock_start = Mock(return_value=1)
    mock_collect = Mock(return_value=companies)
    mock_save = Mock()
    mock_finish = Mock()

    monkeypatch.setattr(corp_list, "start_pipeline_run", mock_start)
    monkeypatch.setattr(corp_list, "get_listed_companies", mock_collect)
    monkeypatch.setattr(corp_list, "save_company_list", mock_save)
    monkeypatch.setattr(corp_list, "finish_pipeline_run", mock_finish)

    corp_list.main()

    mock_save.assert_called_once_with(companies)

    mock_finish.assert_called_once_with(
        run_id=1,
        status="SUCCESS",
        records_processed=1,
    )


def test_pipeline_failure(monkeypatch):
    mock_start = Mock(return_value=2)
    mock_collect = Mock(side_effect=RuntimeError("API connection failed"))
    mock_finish = Mock()

    monkeypatch.setattr(corp_list, "start_pipeline_run", mock_start)
    monkeypatch.setattr(corp_list, "get_listed_companies", mock_collect)
    monkeypatch.setattr(corp_list, "finish_pipeline_run", mock_finish)

    with pytest.raises(RuntimeError, match="API connection failed"):
        corp_list.main()

    mock_finish.assert_called_once_with(
        run_id=2,
        status="FAILED",
        error_message="API connection failed",
    )
