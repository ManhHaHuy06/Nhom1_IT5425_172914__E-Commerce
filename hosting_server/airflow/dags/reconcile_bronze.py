"""Hourly Bronze completeness check: OLTP rows at or below the committed watermark must all be in Bronze.

Separate from ingest_oltp_to_bronze because it reads the key columns of every landed batch of a table (cost grows
with the number of batches); running it every 15 min would slow ingestion. A failure is final (no retry) and the
message says how to re-land the missing rows: move the table's watermark back, the next ingest run reads them again
(versions landed twice are harmless, Silver keeps the latest one per key).
"""
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

from bronze import TRANSACTION_TABLES, watermark_var
from reconcile import KEYS, check_table

SAMPLE = 5


@dag(
    dag_id="reconcile_bronze",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["ingestion", "bronze", "quality"],
)
def reconcile_bronze():
    @task
    def check(table: str):
        watermark = Variable.get(watermark_var(table), default_var=None)
        if watermark is None:
            print(f"{table}: no committed watermark yet, nothing to check")
            return {"table": table, "skipped": True}
        result = check_table(table, watermark)
        missing = result.pop("missing")
        print(f"{table}: watermark={watermark} oltp_rows={result['oltp_rows']} "
              f"bronze_versions={result['bronze_versions']} bronze_objects={result['bronze_objects']} "
              f"missing={len(missing)}")
        if len(missing):
            first = missing["updated_at"].min().to_pydatetime()
            rewind = (first - timedelta(milliseconds=1)).strftime("%Y-%m-%d %H:%M:%S.%f")
            sample = missing.head(SAMPLE)[KEYS[table] + ["updated_at"]].to_dict("records")
            raise AirflowFailException(
                f"{table}: {len(missing)} OLTP row versions with updated_at <= {watermark} are not in Bronze "
                f"(oldest updated_at {first}). Sample: {sample}. To re-land them run "
                f"`airflow variables set {watermark_var(table)} '{rewind}'`, then trigger ingest_oltp_to_bronze "
                f"and this DAG again.")
        return {**result, "missing": 0, "checked_at": datetime.utcnow().isoformat()}

    check.expand(table=TRANSACTION_TABLES)


reconcile_bronze()
