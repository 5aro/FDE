-- 동일 보고서의 동일 계정 행이 중복 저장되는 것을 방지한다.
ALTER TABLE financial_statements
ADD CONSTRAINT uq_financial_statement_row
UNIQUE (
    rcept_no,
    fs_div,
    sj_div,
    ord,
    account_detail
);