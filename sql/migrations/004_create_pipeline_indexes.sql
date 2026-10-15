-- 파이프라인 실행 이력을 빠르게 조회하기 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name_started
ON pipeline_runs (pipeline_name, started_at DESC);