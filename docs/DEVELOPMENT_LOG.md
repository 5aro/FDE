# FDE Development Log

## v1.0 — OpenDART ETL 파이프라인 구축

### 1. 개발 환경

- Python 3.12 / PostgreSQL 17
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
```

- 상장기업 3,998건 적재
- 5개 기업 재무제표 1,281건 적재
- Airflow DAG 정상 실행
- SQL 분석 및 Streamlit 대시보드 구현

---

## v1.1 — Data Quality & Incremental ETL


### 1. 개발 목표

기존 ETL 파이프라인의 데이터 신뢰성과 운영 안정성을 개선한다.

주요 목표:
- 재무제표 수집 상태 관리
- 중복 수집 방지
- 실패 데이터 재처리
- 트랜잭션 기반 데이터 정합성 확보
- pytest 기반 자동화 테스트 강화

### 2. 구현 내용

**재무제표 수집 상태 관리**

- `financial_collection_status` 테이블 생성
- SQL 마이그레이션 `008_create_financial_collection.sql` 적용
- 기업별, 사업연도별, 재무제표 구분별 수집 상태 관리

수집 상태:
- `RUNNING`: 수집 진행 중
- `SUCCESS`: 수집 완료
- `FAILED`: 수집 실패

추가 관리 항목:
- `row_count`: 수집 처리 건수
- `retry_count`: 재시도 횟수
- `started_at`: 수집 시작 시각
- `completed_at`: 수집 완료 시각
- `error_message`: 오류 메시지

**증분 수집 개선**

- `get_collection_status()` 구현
- 기존 수집 상태를 확인한 후 수집 여부 결정
- `SUCCESS` 상태의 기업은 재수집 생략
- `FAILED` 상태의 기업은 재시도 허용
- `RUNNING` 상태의 중복 실행 방지

수집 흐름:

```text
기업별 수집 상태 확인
        ↓
   ┌────┼────┐
   ↓    ↓    ↓
SUCCESS FAILED  RUNNING
   ↓    ↓       ↓
 SKIP  RETRY   ERROR
        ↓
      RUNNING
        ↓
    OpenDART API
        ↓
   재무제표 UPSERT
        ↓
      SUCCESS
```

**트랜잭션 기반 데이터 정합성**

- `save_financial_statements()`에 외부 DB 연결 전달 기능 추가
- `mark_collection_success()` 구현
- 재무제표 UPSERT와 수집 성공 상태 기록을 단일 트랜잭션으로 통합
- 정상 처리 시 COMMIT
- 오류 발생 시 ROLLBACK

트랜잭션 흐름:

```text
BEGIN
  │
  ├── financial_statements UPSERT
  │
  ├── financial_collection_status UPDATE
  │
  ├── 정상 종료 → COMMIT
  │
  └── 오류 발생 → ROLLBACK
```

**데이터베이스 연결 설정 개선**

- `get_db_config()` 기반 DB 연결 설정 통일
- 개발 DB와 테스트 DB 연결 구분
- 테스트 실행 시 `fde_test` 사용
- DB 연결 설정 중복 코드 감소

### 3. 테스트 구현

**PostgreSQL 트랜잭션 통합 테스트**

`tests/test_financial_transaction.py`

구현한 테스트:

1. `test_financial_transaction_rollback`
   - 재무제표 저장 및 상태 변경 후 의도적인 오류 발생
   - 두 작업의 ROLLBACK 검증

2. `test_financial_transaction_commit`
   - 재무제표 저장 및 상태 변경
   - COMMIT 이후 데이터 저장 여부 검증

3. `test_skip_already_collected_financials`
   - SUCCESS 상태의 기업 수집 생략
   - OpenDART API 미호출 검증
   - 재무제표 저장 함수 미호출 검증

4. `test_retry_failed_financial_collection`
   - FAILED 상태의 기업 재수집 경로 검증
   - RUNNING 상태 기록 함수 호출 확인
   - API 및 저장 함수 호출 확인

테스트 방식:
- 실제 PostgreSQL 통합 테스트
- `unittest.mock` 기반 단위 테스트
- pytest 자동화
- 테스트 DB `fde_test` 사용

### 4. 트러블슈팅

**Python 인터프리터 들여쓰기 오류**

문제:
- 대화형 Python 인터프리터에서 다중 `with` 및 `try` 블록 입력 중 SyntaxError 발생

해결:
- 트랜잭션 테스트를 단계별 SQL 실행 방식으로 변경
- 이후 pytest 테스트 코드로 자동화

**pytest 모듈 경로 오류**

문제:

```text
ModuleNotFoundError: No module named 'src'
```

해결:

```bash
DB_NAME=fde_test python -m pytest tests/ -v
```

- Python 모듈 방식으로 pytest 실행
- 프로젝트 루트의 `src` 모듈 인식 문제 해결

**재무제표 계정 중복 조회**

문제:
- 삼성전자 재무제표에서 `account_nm = '자본총계'` 조건으로 2개 행 조회

원인:
- 계정명만으로 재무제표 행을 고유하게 식별할 수 없음

대응:
- 재무제표 UPSERT에 복합 고유 제약조건 사용
- 테스트 과정에서 기존 데이터 변경 사항 ROLLBACK

**트랜잭션 원자성 검증**

문제:
- 재무제표 저장 이후 상태 기록에 오류가 발생하면 데이터 불일치 가능

해결:
- 동일한 PostgreSQL 연결을 사용하도록 함수 개선
- UPSERT와 상태 변경을 하나의 트랜잭션으로 통합
- COMMIT 및 ROLLBACK 통합 테스트 통과

### 5. 테스트 결과

실행 명령어:

```bash
DB_NAME=fde_test python -m pytest tests/ -v
```

최종 결과:

```text
15 passed in 0.22s
```

검증 완료:
- PostgreSQL COMMIT
- PostgreSQL ROLLBACK
- 증분 수집 중복 방지
- 실패 기업 재시도 경로
- 기존 기능 회귀 테스트

### 6. 개발 결과

- 재무제표 수집 상태 관리 체계 구축
- 수집 완료 기업 중복 처리 방지
- 실패 기업 재수집 경로 구현
- 트랜잭션 기반 데이터 정합성 확보
- PostgreSQL 통합 테스트 자동화
- 전체 pytest 15개 통과

### 7. 향후 개선 사항

- 테스트 데이터 생성 및 정리 로직을 pytest fixture로 개선
- 실제 DB 기반 실패 후 재시도 상태 전이 테스트 추가
- 장시간 RUNNING 상태로 남은 작업의 복구 정책 수립
- 재무제표 데이터 품질 검증 항목 확대

---

## v1.2 — Data Modeling & Transformation (Planned)

### 개발 목표

dbt를 활용하여 원천 재무제표 데이터를 분석 목적에 맞게 변환하고 관리한다.

### 주요 개발 계획

**dbt 환경 구축**
- dbt-core 및 PostgreSQL Adapter 설치
- FDE PostgreSQL 연결
- dbt 프로젝트 초기화

**Staging 모델**
- 기업정보 Staging 모델
- 재무제표 Staging 모델
- 컬럼 및 데이터 타입 표준화

**Mart 모델**
- 기업별 재무지표 모델
- 매출액 및 영업이익 분석 모델
- 재무비율 계산 모델

**데이터 품질 테스트**
- not_null
- unique
- relationships
- accepted_values

**기대 결과**
- SQL 변환 로직 모듈화
- 분석용 데이터 모델 재사용
- 데이터 품질 검증 자동화
- Streamlit 대시보드와 dbt Mart 연계 기반 마련