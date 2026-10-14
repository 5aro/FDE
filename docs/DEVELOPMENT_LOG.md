# FDE Development Log

## OpenDART ETL 파이프라인 구축

### 1. 개발 환경
- Python / PostgreSQL 17
- Apache Airflow 3.1.2 / CeleryExecutor
- Docker Compose / Redis
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
- 3개 Task 실행 성공

```text
collect_corporations
        ↓
validate_results
        ↓
check_data_quality
        ↓
      SUCCESS