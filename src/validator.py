from datetime import datetime


def validate_company(company):

    if not company:
        return False

    # 기업 고유번호 검증
    if not company["corp_code"].isdigit():
        return False

    if len(company["corp_code"]) != 8:
        return False

    # 기업명 검증
    if not company["corp_name"]:
        return False

    # 설립일 검증
    try:
        datetime.strptime(company["est_dt"], "%Y%m%d")
    except (ValueError, TypeError):
        return False

    return True
