import os
import csv
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("DART_API_KEY")

url = "https://opendart.fss.or.kr/api/company.json"


# 1. 기업정보 수집 함수
def get_company(corp_code):
    params = {"crtfc_key": API_KEY, "corp_code": corp_code}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        if data.get("status") != "000":
            print(f"[API 오류] {corp_code}: {data.get('message')}")
            return None
        company = {
            "corp_code": data["corp_code"],
            "corp_name": data["corp_name"],
            "stock_code": data["stock_code"],
            "ceo_nm": data["ceo_nm"],
            "est_dt": data["est_dt"],
        }

        return company

    except requests.exceptions.RequestException as e:
        print(f"[네트워크 오류] {corp_code}: {e}")
        return None

    except (ValueError, KeyError) as e:
        print(f"[데이터 처리 오류] {corp_code}: {e}")
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
corp_codes = ["00126380", "00164779"]

companies = []

for corp_code in corp_codes:

    company = get_company(corp_code)

    if company is not None:
        companies.append(company)

print(companies)

save_to_csv(companies)
