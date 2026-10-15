"""
FDE 데이터베이스 마이그레이션 실행기

기능:
1. schema_migrations 테이블 생성
2. 적용된 마이그레이션 조회
3. 미적용 SQL 파일 실행
4. 실행 결과 기록

실행:
    python scripts/migrate.py
"""

import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

# 프로젝트 루트 경로
BASE_DIR = Path(__file__).resolve().parent.parent

# SQL 파일 경로
MIGRATIONS_DIR = BASE_DIR / "sql" / "migrations"
BOOTSTRAP_FILE = BASE_DIR / "sql" / "bootstrap" / "create_schema_migrations.sql"

# .env 파일 로드
load_dotenv(BASE_DIR / ".env")


def run_migrations():
    """미적용 SQL 마이그레이션을 실행한다."""

    # 안전을 위해 기본 실행 대상은 테스트 DB로 설정
    db_name = os.getenv("MIGRATION_DB_NAME", "fde_test")

    # 운영 DB에 실수로 적용하는 것을 방지
    if db_name != "fde_test":
        raise ValueError("현재는 fde_test 데이터베이스에서만 실행할 수 있습니다.")

    # 마이그레이션 파일 검색
    migration_files = sorted(MIGRATIONS_DIR.glob("[0-9]*_*.sql"))

    if not migration_files:
        raise FileNotFoundError("마이그레이션 SQL 파일이 없습니다.")

    with psycopg.connect(
        dbname=db_name,
        user=os.getenv("DB_USER"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        password=os.getenv("DB_PASSWORD"),
    ) as conn:

        # 실제 접속한 데이터베이스 검증
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            actual_db = cur.fetchone()[0]

            if actual_db != "fde_test":
                raise RuntimeError("테스트 DB가 아닌 곳에 연결되었습니다.")

        # 마이그레이션 이력 테이블 생성
        with conn.cursor() as cur:
            bootstrap_sql = BOOTSTRAP_FILE.read_text(encoding="utf-8")
            cur.execute(bootstrap_sql)

        # 적용된 마이그레이션 조회
        with conn.cursor() as cur:
            cur.execute("SELECT version FROM schema_migrations")

            applied_versions = {row[0] for row in cur.fetchall()}

        # SQL 파일을 순서대로 실행
        for file in migration_files:
            version, description = file.stem.split("_", 1)

            if version in applied_versions:
                print(f"[SKIP] {file.name}")
                continue

            print(f"[RUN] {file.name}")

            sql = file.read_text(encoding="utf-8")

            # SQL 실행 + 이력 기록
            # 하나라도 실패하면 해당 트랜잭션이 롤백된다.
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(sql)

                    cur.execute(
                        """
                        INSERT INTO schema_migrations (
                            version,
                            description
                        )
                        VALUES (%s, %s)
                        """,
                        (version, description),
                    )

            print(f"[DONE] {file.name}")

    print("\n모든 마이그레이션 처리 완료")


if __name__ == "__main__":
    run_migrations()
