from datetime import datetime, timedelta
from textwrap import dedent
from airflow.sdk import dag, task, task_group
from assets import bronze_complaints


DBT_PROJECT_DIR = "/opt/dbt"
DBT_PROFILES_DIR = "/home/airflow/.dbt"
DBT_FLAGS = f"--profiles-dir {DBT_PROFILES_DIR} --target dev"


def dbt_command(subcommand: str) -> str:
    """Build a fully-qualified dbt shell command.
    
    Centralizing this means if we ever change the project layout or add
    global flags, it's one edit instead of editing every task.
    """
    return f"cd {DBT_PROJECT_DIR} && dbt {subcommand} {DBT_FLAGS}"

@dag(
    dag_id="nyc311_dbt_pipeline",
    description="ELT pipeline for NYC 311 complaints — silver + gold + snapshots",
    tags=["nyc311", "dbt", "elt", "production"],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    schedule=[bronze_complaints],
    max_active_runs=1,
    doc_md=__doc__,
    default_args={
        "owner": "tihim",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    },
)
def nyc311_dbt_pipeline():
    """ELT pipeline orchestrating the full dbt build for NYC 311 data."""


    @task.bash(
        task_id="install_dbt_packages",
        doc_md="Install dbt packages declared in `packages.yml`. Idempotent.",
    )
    def install_dbt_packages() -> str:
        return dbt_command("deps")

    @task.bash(
        task_id="check_source_freshness",
        doc_md=dedent("""
            Verify that bronze data is fresh enough to proceed.
            
            Configured to warn-not-fail (via `|| true`) — a stale source
            should be visible in logs but shouldn't block scheduled builds.
            Real-world pipelines often process whatever bronze has, even
            if it's slightly stale, rather than skipping a day entirely.
        """),
    )
    def check_source_freshness() -> str:
        return dbt_command("source freshness") + " || true"


    @task_group(group_id="build_silver")
    def build_silver():
        """Build the silver layer: staging + intermediate models."""

        @task.bash(
            task_id="build_staging",
            doc_md="Flatten JSON, dedupe complaints, normalize text fields.",
        )
        def build_staging() -> str:
            return dbt_command("build --select staging")

        @task.bash(
            task_id="build_intermediate",
            doc_md="Apply business logic: response time, resolution flags, age buckets.",
        )
        def build_intermediate() -> str:
            return dbt_command("build --select intermediate")

        build_staging() >> build_intermediate()



    @task_group(group_id="build_gold")
    def build_gold():
        """Build the gold layer: seeds → dimensions → fact → reporting."""

        @task.bash(
            task_id="load_seeds",
            doc_md="Load reference CSVs (borough metadata, complaint type categories).",
        )
        def load_seeds() -> str:
            return dbt_command("seed")

        @task.bash(
            task_id="build_dimensions",
            doc_md="Build conformed dimensions with surrogate keys and Unknown sentinels.",
        )
        def build_dimensions() -> str:
            return dbt_command("build --select dim_borough dim_complaint_type")

        @task.bash(
            task_id="build_facts",
            doc_md=dedent("""
                Build fct_complaints — incremental fact table.
                
                Only new rows (extracted_at > MAX in target) are processed
                via MERGE on complaint_id. Saves Snowflake credits at scale.
            """),
        )
        def build_facts() -> str:
            return dbt_command("build --select fct_complaints")

        @task.bash(
            task_id="build_reporting",
            doc_md="Build pre-aggregated reporting marts (e.g. dashboard_daily_summary).",
        )
        def build_reporting() -> str:
            return dbt_command("build --select marts.reporting")

        load_seeds() >> build_dimensions() >> build_facts() >> build_reporting()


    @task.bash(
        task_id="refresh_snapshots",
        doc_md=dedent("""
            Refresh SCD Type 2 history. Captures status transitions,
            closure events, and any other tracked field changes since
            the last run. Append-only — never overwrites history.
        """),
    )
    def refresh_snapshots() -> str:
        return dbt_command("snapshot")


    (
        install_dbt_packages()
        >> check_source_freshness()
        >> build_silver()
        >> build_gold()
        >> refresh_snapshots()
    )

nyc311_dbt_pipeline()