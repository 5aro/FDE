import csv
import logging
import os
import time

import requests
from dotenv import load_dotenv

from database import save_companies
from validator import validate_company

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

load_dotenv()

API_KEY = os.getenv("DART_API_KEY")

url = "https://opendart.fss.or.kr/api/company.json"


# 1. 기업정보 수집 함수
def get_company(corp_code):
    params = {
        "crtfc_key": API_KEY,
        "corp_code": corp_code,
    }

    max_retries = 3

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=10,
            )

            response.raise_for_status()
            data = response.json()

            if data.get("status") != "000":
                logger.error(
                    f"OpenDART API 오류: {corp_code}, "
                    f"status={data.get('status')}, "
                    f"message={data.get('message')}"
                )
                return None

            return {
                "corp_code": data["corp_code"],
                "corp_name": data["corp_name"],
                "stock_code": data["stock_code"],
                "ceo_nm": data["ceo_nm"],
                "est_dt": data["est_dt"],
            }

        except requests.exceptions.RequestException as e:

            retryable = (
                not isinstance(e, requests.exceptions.HTTPError)
                or e.response is not None
                and e.response.status_code in (500, 502, 503, 504)
            )

            if not retryable:
                logger.error(f"재시도 불가능한 오류: {corp_code}, {e}")
                return None

            if attempt == max_retries:
                logger.error(f"최종 수집 실패: {corp_code}, {e}")
                return None

            wait_seconds = 2 ** (attempt - 1)

            logger.warning(
                f"API 요청 실패: {corp_code}, "
                f"시도={attempt}/{max_retries}, "
                f"{wait_seconds}초 후 재시도"
            )

            time.sleep(wait_seconds)

        except (ValueError, KeyError) as e:
            logger.error(f"데이터 처리 오류: {corp_code}, {e}")
            return None


# 2. csv 저장 함수
def save_to_csv(companies):

    if not companies:
        print("저장할 데이터가 없습니다.")
        return

    with open("data/companies.csv", "w", newline="", encoding="utf-8-sig") as file:

        writer = csv.DictWriter(file, fieldnames=companies[0].keys())

        writer.writeheader()

        writer.writerows(companies)

    print(f"총 {len(companies)}개 기업 저장 완료")


# 3. 메인 실행 로직
def main():
    corp_codes = ["00126380", "00164779"]
    companies = []

    logger.info("기업정보 수집 시작")

    for corp_code in corp_codes:
        company = get_company(corp_code)

        if company is None:
            logger.warning(f"기업정보 수집 실패: {corp_code}")
            continue

        if not validate_company(company):
            logger.warning(f"기업정보 검증 실패: {corp_code}")
            continue

        companies.append(company)
        logger.info(f"기업정보 수집 성공: {company['corp_name']}")

    save_companies(companies)

    logger.info(f"기업정보 수집 종료: {len(companies)}개")


if __name__ == "__main__":
    main()
