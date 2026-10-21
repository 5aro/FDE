import io
import logging
import os
import xml.etree.ElementTree as ET
import zipfile

import requests
from dotenv import load_dotenv

from database import (
    finish_pipeline_run,
    save_company_list,
    start_pipeline_run,
)

load_dotenv()

logger = logging.getLogger(__name__)

API_KEY = os.getenv("DART_API_KEY")

URL = "https://opendart.fss.or.kr/api/corpCode.xml"


def get_listed_companies():
    if not API_KEY:
        raise ValueError("DART_API_KEY가 설정되지 않았습니다.")

    response = requests.get(
        URL,
        params={"crtfc_key": API_KEY},
        timeout=30,
    )
    response.raise_for_status()

    # ZIP 파일을 메모리에서 처리
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        xml_name = next(
            name for name in archive.namelist() if name.lower().endswith(".xml")
        )

        xml_data = archive.read(xml_name)

    # XML 파싱
    root = ET.fromstring(xml_data)

    companies = []

    for item in root.findall("list"):
        corp_code = item.findtext("corp_code")
        corp_name = item.findtext("corp_name")
        stock_code = item.findtext("stock_code")

        # 종목코드가 없는 비상장기업 제외
        if not stock_code or not stock_code.strip():
            continue

        companies.append(
            {
                "corp_code": corp_code,
                "corp_name": corp_name,
                "stock_code": stock_code,
            }
        )

    logger.info("상장기업 %s개 추출 완료", len(companies))

    return companies


def main():
    run_id = start_pipeline_run("corp_list")

    try:
        companies = get_listed_companies()

        # 증분 적재 실행 및 결과 수집
        metrics = save_company_list(companies)

        # 실행 이력 저장
        finish_pipeline_run(
            run_id=run_id,
            status="SUCCESS",
            records_processed=metrics["processed"],
            records_inserted=metrics["inserted"],
            records_updated=metrics["updated"],
            records_unchanged=metrics["unchanged"],
        )

        logger.info(
            "증분 적재 완료 | 전체: %s | 신규: %s | 변경: %s | 동일: %s",
            metrics["processed"],
            metrics["inserted"],
            metrics["updated"],
            metrics["unchanged"],
        )

    except Exception as e:
        logger.exception("기업 목록 수집 실패")

        finish_pipeline_run(
            run_id=run_id,
            status="FAILED",
            error_message=str(e),
        )
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
