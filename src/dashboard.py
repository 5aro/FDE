"""
FDE 재무 데이터 분석 대시보드

PostgreSQL에 저장된 기업별 재무제표를 조회하고,
Streamlit으로 분석 결과를 시각화한다.
"""

import os
from pathlib import Path

import pandas as pd
import psycopg
import streamlit as st
from dotenv import load_dotenv

# --------------------------------------------------
# 1. 환경변수 로드
# --------------------------------------------------

# 프로젝트 최상위 디렉터리
BASE_DIR = Path(__file__).resolve().parent.parent

# 프로젝트 루트의 .env 파일 로드
load_dotenv(BASE_DIR / ".env")


# --------------------------------------------------
# 2. Streamlit 페이지 설정
# --------------------------------------------------

st.set_page_config(
    page_title="FDE 재무 분석",
    page_icon="📊",
    layout="wide",
)

st.title("📊 FDE 기업 재무 분석")
st.caption("OpenDART API · PostgreSQL · Airflow · Streamlit")


# --------------------------------------------------
# 3. PostgreSQL 데이터 조회
# --------------------------------------------------


@st.cache_data(ttl=300)
def load_financial_data():
    """
    SQL 파일을 읽어 PostgreSQL에서 실행한다.

    ttl=300:
        조회 결과를 5분 동안 캐싱하여
        불필요한 DB 요청을 줄인다.
    """

    # 기존에 작성한 분석 SQL 파일 경로
    sql_path = BASE_DIR / "sql" / "queries" / "financial_analysis.sql"

    # SQL 파일 읽기
    query = sql_path.read_text(encoding="utf-8")

    # PostgreSQL 연결
    with psycopg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "fde"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
    ) as conn:

        # SQL 실행 후 조회 결과를 DataFrame으로 변환
        with conn.cursor() as cur:
            cur.execute(query)

            columns = [column.name for column in cur.description]
            rows = cur.fetchall()

    return pd.DataFrame(rows, columns=columns)


try:
    df = load_financial_data()

except Exception as e:
    st.error(f"데이터 조회 실패: {e}")
    st.stop()


# --------------------------------------------------
# 4. 데이터 확인
# --------------------------------------------------

if df.empty:
    st.warning("조회된 재무 데이터가 없습니다.")
    st.stop()

# PostgreSQL NUMERIC 타입을 그래프용 숫자로 변환
numeric_columns = [
    "매출액_조원",
    "영업이익_조원",
    "당기순이익_조원",
    "영업이익률",
]

for column in numeric_columns:
    df[column] = pd.to_numeric(df[column], errors="coerce")


# --------------------------------------------------
# 5. 재무 데이터 테이블
# --------------------------------------------------

st.subheader("기업별 재무지표")

st.dataframe(
    df,
    use_container_width=True,
    hide_index=True,
)


# --------------------------------------------------
# 6. 매출액 비교
# --------------------------------------------------

st.subheader("기업별 매출액 비교 (조 원)")

st.bar_chart(
    df.set_index("기업명")["매출액_조원"],
)


# --------------------------------------------------
# 7. 영업이익 비교
# --------------------------------------------------

st.subheader("기업별 영업이익 비교 (조 원)")

st.bar_chart(
    df.set_index("기업명")["영업이익_조원"],
)


# --------------------------------------------------
# 8. 영업이익률 비교
# --------------------------------------------------

st.subheader("기업별 영업이익률 비교 (%)")

st.bar_chart(
    df.set_index("기업명")["영업이익률"],
)
