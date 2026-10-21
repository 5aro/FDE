"""
FDE 데이터베이스 마이그레이션 실행기

기능:
1. 마이그레이션 이력 테이블 생성
2. 기존 마이그레이션 적용 여부 확인
3. 미적용 SQL 파일 실행
4. 마이그레이션별 트랜잭션 처리
5. 테스트 DB / 실제 DB 실행 환경 구분
"""

import os
import sys
from pathlib import Path

import psycopg

# 프로젝트 루트
BASE_DIR = Path(__file__).resolve().parent.parent

# SQL 파일 경로
MIGRATIONS_DIR = BASE_DIR / "sql" / "migrations"
BOOTSTRAP_FILE = BASE_DIR / "sql" / "bootstrap" / "create_schema_migrations.sql"

sys.path.insert(0, str(BASE_DIR / "src"))
from db_config import get_db_config


def run_migrations():

    # Shared target; production migration still requires explicit opt-in.
    db_config = get_db_config()
    db_name = os.getenv("MIGRATION_DB_NAME", db_config["dbname"])
    db_config["dbname"] = db_name

    # 허용된 데이터베이스
    allowed_databases = {"fde_test", "fde"}

    if db_name not in allowed_databases:
        raise ValueError(f"허용되지 않은 데이터베이스: {db_name}")

    # 실제 DB 실행 시 명시적인 승인 필요
    if db_name == "fde" and os.getenv("ALLOW_PRODUCTION_MIGRATION") != "true":
        raise RuntimeError("fde 마이그레이션은 명시적인 승인이 필요합니다.")

    migration_files = sorted(MIGRATIONS_DIR.glob("[0-9]*_*.sql"))

    if not migration_files:
        raise FileNotFoundError("마이그레이션 SQL 파일이 없습니다.")

    print(f"\n마이그레이션 대상 DB: {db_name}\n")

    # autocommit=True:
    # 각 마이그레이션을 독립적인 트랜잭션으로 실행하기 위함
    with psycopg.connect(**db_config, autocommit=True) as conn:

        # 실제 연결 DB 확인
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            actual_db = cur.fetchone()[0]

        if actual_db != db_name:
            raise RuntimeError(f"DB 연결 불일치: " f"예상={db_name}, 실제={actual_db}")

        # 실제 DB의 기존 스키마를 먼저 베이스라인으로
        # 등록해야 하므로, 미등록 상태에서는 실행 차단
        if db_name == "fde":
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                          AND table_name = 'schema_migrations'
                    )
                    """)
                history_table_exists = cur.fetchone()[0]

            if not history_table_exists:
                raise RuntimeError(
                    "fde에 마이그레이션 이력이 없습니다. "
                    "기존 스키마 검증 및 베이스라인 등록이 "
                    "먼저 필요합니다."
                )

        # 이력 테이블 생성
        bootstrap_sql = BOOTSTRAP_FILE.read_text(encoding="utf-8")

        with conn.transaction(), conn.cursor() as cur:
            cur.execute(bootstrap_sql)

        # 적용된 버전 조회
        with conn.cursor() as cur:
            cur.execute("SELECT version FROM public.schema_migrations")

            applied_versions = {row[0] for row in cur.fetchall()}

        # 실제 DB에서는 기존 001~003의 베이스라인을 확인
        if db_name == "fde":
            required_baseline = {"001", "002", "003"}

            if not required_baseline.issubset(applied_versions):
                raise RuntimeError("fde의 001~003 베이스라인이 " "완료되지 않았습니다.")

        # 마이그레이션 순차 실행
        for file in migration_files:

            version, description = file.stem.split("_", 1)

            if version in applied_versions:
                print(f"[SKIP] {file.name}")
                continue

            print(f"[RUN] {file.name}")

            sql = file.read_text(encoding="utf-8")

            # SQL 실행 + 이력 저장을 하나의 트랜잭션으로 처리
            with conn.transaction(), conn.cursor() as cur:

                cur.execute(sql)

                cur.execute(
                    """
                        INSERT INTO public.schema_migrations (
                            version,
                            description
                        )
                        VALUES (%s, %s)
                        """,
                    (version, description),
                )

            applied_versions.add(version)

            print(f"[DONE] {file.name}")

    print("\n모든 마이그레이션 처리 완료")


if __name__ == "__main__":
    run_migrations()
