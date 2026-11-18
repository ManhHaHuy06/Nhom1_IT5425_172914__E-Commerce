"""Scheduled Olist OLTP -> MinIO staging -> Lakehouse Bronze/Silver/Gold -> ClickHouse."""
from __future__ import annotations

from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import text

from clickhouse_loader import refresh_clickhouse
from ingestion import TRANSACTIONAL, extract_reference, extract_transactions, make_batch_id, stage_batch
from local_cleanup import cleanup_run_extracts
from oltp import engine
from spark_runner_client import run_spark

WORK_ROOT = "/opt/airflow/work"
EPOCH = "1970-01-01 00:00:00.000000"


@dag(
    dag_id="olist_end_to_end_pipeline",
    description="OLTP -> MinIO staging -> Iceberg Bronze -> Silver -> Gold -> ClickHouse",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["olist", "oltp", "bronze", "silver", "gold", "clickhouse"],
)
def olist_end_to_end_pipeline():
    @task(task_id="check_ingestion_schedule")
    def check_ingestion_schedule(**context):
        with engine().connect() as conn:
            wm_to_dt = conn.execute(text("SELECT NOW(3) - INTERVAL 10 SECOND")).scalar()
        wm_to = wm_to_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        windows = {
            table: {"from": Variable.get(f"wm__{table}", default_var=EPOCH), "to": wm_to}
            for table in TRANSACTIONAL
        }
        last_snapshot = Variable.get("last_reference_snapshot_date", default_var="")
        previous_geo_hash = Variable.get("geolocation_snapshot_sha256", default_var="")
        logical_date = context["ds"]
        reference_due = last_snapshot != logical_date
        plan = {
            "wm_to": wm_to,
            "transaction_windows": windows,
            "transaction_batch_id": make_batch_id(windows),
            "reference_due": reference_due,
            "snapshot_id": logical_date,
            "previous_geolocation_sha256": previous_geo_hash,
            "run_id": context["run_id"],
        }
        print(f"[PLAN] transaction_batch={plan['transaction_batch_id']} reference_due={reference_due} snapshot={logical_date}")
        return plan

    @task(task_id="extract_transactions")
    def extract_transaction_task(plan: dict):
        return extract_transactions(plan, WORK_ROOT, plan["run_id"])

    @task(task_id="stage_transactions")
    def stage_transaction_task(result: dict):
        if result["total_rows"] == 0:
            return {**result, "staged": False, "source_uri": None}
        prefix = f"staging/bronze/oltp/batch_id={result['batch_id']}"
        return stage_batch(result, prefix)

    @task(task_id="load_transaction_bronze")
    def load_transaction_bronze_task(staged: dict):
        if staged["total_rows"] == 0:
            return {"loaded": False, "rows": 0, "batch_id": staged["batch_id"]}
        run_spark("bronze-transactional", staged["source_uri"], staged["batch_id"])
        return {"loaded": True, "rows": staged["total_rows"], "batch_id": staged["batch_id"]}

    @task(task_id="extract_reference_snapshot")
    def extract_reference_task(plan: dict):
        return extract_reference(plan, WORK_ROOT, plan["run_id"])

    @task(task_id="stage_reference_snapshot")
    def stage_reference_task(result: dict):
        if not result.get("due"):
            return result
        prefix = f"staging/bronze/reference/snapshot_id={result['snapshot_id']}"
        return stage_batch(result, prefix)

    @task(task_id="load_reference_bronze")
    def load_reference_bronze_task(staged: dict):
        if not staged.get("due"):
            return {"loaded": False, "rows": 0, "snapshot_id": None}
        skip_tables = ["geolocation"] if staged.get("geolocation_already_loaded") else []
        batch_id = f"reference_{staged['snapshot_id'].replace('-', '')}"
        run_spark("bronze-reference", staged["source_uri"], batch_id, skip_tables=skip_tables)
        return {"loaded": True, "rows": staged["total_rows"], "snapshot_id": staged["snapshot_id"]}

    @task(task_id="bronze_completion")
    def bronze_completion_task(tx_loaded: dict, ref_loaded: dict):
        has_new_data = int(tx_loaded["rows"]) > 0 or int(ref_loaded["rows"]) > 0
        if not has_new_data:
            raise AirflowSkipException("No new transactional data and no due reference snapshot")
        return {"ready": True, "transaction_rows": tx_loaded["rows"], "reference_rows": ref_loaded["rows"]}

    @task(task_id="build_silver_dimensions")
    def build_silver_dimensions_task(_ready: dict):
        return run_spark("silver-dimensions")

    @task(task_id="build_silver_facts")
    def build_silver_facts_task(_dimensions_result: dict):
        return run_spark("silver-facts")

    @task(task_id="build_gold_marts")
    def build_gold_marts_task(_facts_result: dict):
        return run_spark("gold")

    @task(task_id="refresh_clickhouse")
    def refresh_clickhouse_task(_gold_result: dict):
        return refresh_clickhouse()

    @task(task_id="commit_ingestion_state", trigger_rule=TriggerRule.ALL_DONE)
    def commit_ingestion_state_task(**context):
        from airflow.utils.session import create_session
        from state_commit import build_ingestion_state_updates

        ti = context["ti"]
        instances = context["dag_run"].get_task_instances()
        failed = [
            item.task_id for item in instances
            if item.task_id != "commit_ingestion_state" and item.state in {"failed", "upstream_failed"}
        ]
        plan = ti.xcom_pull(task_ids="check_ingestion_schedule")
        tx_extract = ti.xcom_pull(task_ids="extract_transactions")
        ref_extract = ti.xcom_pull(task_ids="extract_reference_snapshot")
        tx_loaded = ti.xcom_pull(task_ids="load_transaction_bronze")
        ref_loaded = ti.xcom_pull(task_ids="load_reference_bronze")
        if not all([plan, tx_extract, ref_extract, tx_loaded, ref_loaded]):
            raise AirflowException("Missing ingestion XCom; refusing watermark commit")

        clickhouse_ti = next((item for item in instances if item.task_id == "refresh_clickhouse"), None)
        updates = build_ingestion_state_updates(
            plan=plan,
            tx_loaded=tx_loaded,
            ref_loaded=ref_loaded,
            reference_extract=ref_extract,
            failed_tasks=failed,
            clickhouse_succeeded=bool(clickhouse_ti and clickhouse_ti.state == "success"),
            success_at=datetime.utcnow().isoformat(timespec="seconds") + "Z",
        )
        # All validation occurs before writes; persist all watermark Variables in one metadata DB transaction.
        with create_session() as session:
            for key, value in updates.items():
                Variable.set(key, value, session=session)
        print(f"[COMMIT] Updated {len(updates)} ingestion state variables atomically")
        return {"committed": True, "updated_variables": sorted(updates)}

    @task(task_id="cleanup_local_extracts", trigger_rule=TriggerRule.ALL_SUCCESS)
    def cleanup_local_extracts_task(_commit_result: dict, plan: dict, tx_extract: dict, ref_extract: dict):
        return cleanup_run_extracts(WORK_ROOT, plan["run_id"], tx_extract, ref_extract)

    plan = check_ingestion_schedule()
    tx_extract = extract_transaction_task(plan)
    tx_stage = stage_transaction_task(tx_extract)
    tx_bronze = load_transaction_bronze_task(tx_stage)

    ref_extract = extract_reference_task(plan)
    ref_stage = stage_reference_task(ref_extract)
    ref_bronze = load_reference_bronze_task(ref_stage)

    # Only one Spark write job at a time; Bronze transaction and reference loads are serialized.
    tx_bronze >> ref_bronze
    ready = bronze_completion_task(tx_bronze, ref_bronze)
    dimensions = build_silver_dimensions_task(ready)
    facts = build_silver_facts_task(dimensions)
    gold = build_gold_marts_task(facts)
    clickhouse = refresh_clickhouse_task(gold)

    committed = commit_ingestion_state_task()
    [tx_bronze, ref_bronze, clickhouse] >> committed
    cleanup_local_extracts_task(committed, plan, tx_extract, ref_extract)


olist_end_to_end_pipeline()
