# FDE Development Log

## OpenDART ETL 파이프라인 구축

### 1. 개발 환경
- Python / PostgreSQL 17
- Apache Airflow 3.1.2 / CeleryExecutor
- Docker Compose / Redis
- pandas / Streamlit
- pytest / Git

### 2. 구현 내용

**데이터 수집 및 적재**
- OpenDART `corpCode.xml` API 연동
- ZIP 압축 해제 및 XML 파싱
- 기업정보 3,998건 PostgreSQL 적재
- `ON CONFLICT` 기반 UPSERT 구현
- `pipeline_runs` 테이블을 통한 실행 이력 관리

**Airflow 자동화**
- Docker 기반 Airflow 실행 환경 구축
- Airflow 메타데이터 DB와 FDE DB 분리
- `fde_daily_pipeline` DAG 구현
- 4개 Task 실행 성공

**재무제표 수집**
- OpenDART 재무제표 API 연동
- 5개 기업의 2025년 연결재무제표 수집
- 재무제표 1,281건 PostgreSQL 적재
- UPSERT를 통한 중복 데이터 방지
- 기존 데이터 확인을 통한 기본 증분 수집 구현

**SQL 데이터 분석**
- 기업별 매출액, 영업이익, 당기순이익 조회
- 영업이익률 계산
- CTE, JOIN, GROUP BY, CASE WHEN 활용
- IS/CIS 재무제표 구조 차이로 인한 데이터 누락 해결
- 분석 SQL을 `sql/queries/financial_analysis.sql`에 저장

**Streamlit 대시보드**
- PostgreSQL 데이터 조회 및 pandas 변환
- 기업별 재무지표 테이블 구현
- 매출액, 영업이익, 영업이익률 비교 그래프 구현
- Streamlit 대시보드 실행 확인

### 3. 트러블슈팅
- Docker 내부 PostgreSQL 연결 오류 해결
- Airflow JWT 인증 오류 해결
- 기업별 재무제표 구조 차이에 따른 SQL 조회 누락 해결

### 4. 실행 결과

```text
collect_corporations
        ↓
validate_results
        ↓
check_data_quality
        ↓
collect_financial_statements
        ↓
      SUCCESS