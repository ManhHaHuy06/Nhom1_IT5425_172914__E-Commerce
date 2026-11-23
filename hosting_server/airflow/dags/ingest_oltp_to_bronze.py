"""Incremental ingestion OLTP -> MinIO Bronze (every 15 min).

Window (wm_from, wm_to], wm_to = DB NOW() - 10s (so uncommitted rows are not skipped).
The watermark is committed only after every table and the manifest were written, so a failed run is
simply re-read from the same window. Object keys derive from the window START (wm_from): it only moves
forward when a window is committed, so a retry of the same window overwrites its own files, while two
different runs (scheduled vs manual, same data_interval) can never overwrite each other's data.
"""
from datetime import timedelta

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
from sqlalchemy import text

from bronze import TRANSACTION_TABLES as TABLES
from bronze import incremental_prefix, land, manifest_key, stamp, watermark_var
from oltp import engine
from s3_utils import put_json

BRONZE_READY = Dataset("s3://olist-lake/bronze/")
CHUNK = 100_000
LAG_SECONDS = 10
EPOCH = "1970-01-01 00:00:00"
TS_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


@dag(
    dag_id="ingest_oltp_to_bronze",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
    tags=["ingestion", "bronze"],
)
def ingest_oltp_to_bronze():
    @task
    def get_window():
        with engine().connect() as c:
            wm_to = c.execute(text(f"SELECT NOW(3) - INTERVAL {LAG_SECONDS} SECOND")).scalar()
        wm_to = wm_to.strftime(TS_FORMAT)
        return {t: {"from": Variable.get(watermark_var(t), EPOCH), "to": wm_to} for t in TABLES}

    @task
    def extract_load(table: str, window: dict):
        w = window[table]
        prefix = incremental_prefix(table, ingest_date=w["to"][:10].replace("-", ""), batch_id=stamp(w["from"]))
        result = land(
            table,
            f"SELECT * FROM {table} WHERE updated_at > :a AND updated_at <= :b ORDER BY updated_at",
            prefix,
            meta={"_source_table": table, "_batch_id": stamp(w["from"]), "_op": "upsert"},
            chunksize=CHUNK,
            params={"a": w["from"], "b": w["to"]},
        )
        return {**result, "from": w["from"], "to": w["to"]}

    @task
    def write_manifest(results: list, window: dict):
        batch_id = stamp(window[TABLES[0]]["to"])
        manifest = {"batch_id": batch_id, "tables": list(results),
                    "total_rows": sum(r["rows"] for r in results)}
        put_json(manifest, manifest_key(batch_id))
        return manifest["total_rows"]

    @task(outlets=[BRONZE_READY])
    def commit_watermark(window: dict, total_rows: int):
        for t, w in window.items():
            Variable.set(watermark_var(t), w["to"])
        return total_rows

    window = get_window()
    results = extract_load.partial(window=window).expand(table=TABLES)
    commit_watermark(window, write_manifest(results, window))


ingest_oltp_to_bronze()
