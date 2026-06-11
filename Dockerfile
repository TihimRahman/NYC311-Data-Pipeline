FROM apache/airflow:3.0.3

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-standard \
    dbt-core==1.10.9 \
    dbt-snowflake==1.10.2 \
    boto3 \
    requests 