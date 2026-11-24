"""Hourly cross-layer Lakehouse ETL orchestration: Landing -> Bronze -> Silver (3NF + Quarantine) -> Gold -> ClickHouse.

Scheduled runs short-circuit when no new landing manifest has been committed since the last completed run.
Manual runs (or dag_run.conf={"force": true}) always run the full medallion pipeline.
All Spark tasks share Airflow pool ``spark_pool`` (1 slot) so concurrent stages never exceed the 6 GB worker.
"""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from lakehouse_runner import latest_landing_manifest, run_clickhouse_branch, run_spark_stage

CURSOR_VAR = "lakehouse__last_manifest_batch"
SPARK_POOL = "spark_pool"


@dag(
    dag_id="etl_lakehouse_medallion",
    # Offset 5 min past :00 so ingest_oltp_to_landing (*/15) finishes & commits before Bronze reads Landing.
    schedule="5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse", "medallion", "etl", "serving"],
)
def etl_lakehouse_medallion():
    @task
    def detect_manifest(**ctx) -> str | None:
        return latest_landing_manifest()

    @task.short_circuit
    def check_new_data(manifest: str | None, **ctx) -> bool:
        dag_run = ctx["dag_run"]
        force = bool((dag_run.conf or {}).get("force")) or dag_run.run_type == "manual"
        last = Variable.get(CURSOR_VAR, default_var=None)
        print(f"manifest={manifest} last_processed={last} force={force} run_type={dag_run.run_type}")
        if force:
            return True
        return bool(manifest and manifest != last)

    with TaskGroup("medallion_core") as medallion_core:
        @task(pool=SPARK_POOL)
        def ddl():
            run_spark_stage("ddl")

        @task(pool=SPARK_POOL)
        def bronze(**ctx):
            mode = (ctx["dag_run"].conf or {}).get("bronze_mode", "append")
            run_spark_stage("bronze", mode=mode, batch_id=ctx["ts_nodash"])

        @task(pool=SPARK_POOL)
        def silver_entities(**ctx):
            run_spark_stage("silver-entities", batch_id=ctx["ts_nodash"])

        @task(pool=SPARK_POOL)
        def silver_transactions(**ctx):
            run_spark_stage("silver-transactions", batch_id=ctx["ts_nodash"])

        ddl() >> bronze() >> silver_entities() >> silver_transactions()

    with TaskGroup("serving_market") as serving_market:
        @task(pool=SPARK_POOL)
        def gold_market():
            run_spark_stage("gold-market")

        @task
        def clickhouse_market():
            run_clickhouse_branch("market")

        gold_market() >> clickhouse_market()

    with TaskGroup("serving_seller") as serving_seller:
        @task(pool=SPARK_POOL)
        def gold_seller():
            run_spark_stage("gold-seller")

        @task
        def clickhouse_seller():
            run_clickhouse_branch("seller")

        gold_seller() >> clickhouse_seller()

    @task
    def commit_etl_cursor(manifest: str | None):
        if manifest:
            Variable.set(CURSOR_VAR, manifest)
            print(f"Committed {CURSOR_VAR}={manifest}")
        return manifest

    manifest = detect_manifest()
    gate = check_new_data(manifest)
    gate >> medallion_core >> [serving_market, serving_seller] >> commit_etl_cursor(manifest)


etl_lakehouse_medallion()
