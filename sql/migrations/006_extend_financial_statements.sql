-- 재무제표 구분: BS(재무상태표), IS(손익계산서) 등
ALTER TABLE financial_statements
ADD COLUMN IF NOT EXISTS sj_div VARCHAR(10);

-- 계정 상세 정보
ALTER TABLE financial_statements
ADD COLUMN IF NOT EXISTS account_detail TEXT;

-- 재무제표 내 계정 표시 순서
ALTER TABLE financial_statements
ADD COLUMN IF NOT EXISTS ord INTEGER;

-- 통화
ALTER TABLE financial_statements
ADD COLUMN IF NOT EXISTS currency VARCHAR(3);

-- 보고서 접수번호
ALTER TABLE financial_statements
ADD COLUMN IF NOT EXISTS rcept_no VARCHAR(14);