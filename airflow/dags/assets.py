from airflow.sdk import Asset



bronze_complaints = Asset(
    name="bronze_complaints",
    uri="snowflake://nyc311/bronze/complaints_raw"
)