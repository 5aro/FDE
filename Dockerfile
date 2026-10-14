FROM apache/airflow:3.1.2

USER airflow

RUN pip install --no-cache-dir \
    "psycopg[binary]>=3.2,<4" \
    "requests>=2.32,<3" \
    "python-dotenv>=1,<2"