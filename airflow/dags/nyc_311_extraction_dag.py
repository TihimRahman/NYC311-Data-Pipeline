from datetime import datetime, timedelta
from textwrap import dedent
import os
from airflow.sdk import dag, task
from assets import bronze_complaints

INGESTION_DIR = "/opt/ingestion"
EXTRACTOR_SCRIPT = "nyc311_extractor.py"



@dag(
    dag_id="nyc311_extraction_pipeline",
    description="Pull NYC 311 complaints from Socrata API → S3 → bronze",
    tags=["nyc311", "extraction", "ingestion", "production"],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    schedule="0 5 * * *",
    max_active_runs=1,
    doc_md=__doc__,
    default_args={
        "owner": "tihim",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=1),
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    },
)
def nyc311_extraction_pipeline():

    @task.bash(task_id="extract_complaints")
    def extract_complaints() -> str:
        return f"cd {INGESTION_DIR} && python {EXTRACTOR_SCRIPT}"

    @task(task_id="load_to_bronze", outlets=[bronze_complaints])
    def load_to_bronze():
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.environ["SNOWFLAKE_ROLE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            schema="BRONZE",
        )
        try:
            cursor = conn.cursor()
            cursor.execute("""
                COPY INTO complaints_raw (raw_data, filename)
                FROM (
                    SELECT $1, METADATA$FILENAME
                    FROM @nyc311.bronze.s3_stage
                )
                FILE_FORMAT = (TYPE = 'JSON', STRIP_OUTER_ARRAY = TRUE)
            """)
            results = cursor.fetchall()
            rows_loaded = sum(r[3] for r in results if r[3])
            print(f"[BRONZE LOAD] COPY INTO complete — {rows_loaded} rows loaded across {len(results)} file(s)")
        finally:
            conn.close()

    extract_complaints() >> load_to_bronze()


nyc311_extraction_pipeline()