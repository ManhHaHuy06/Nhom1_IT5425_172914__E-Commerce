"""Incremental ingestion OLTP -> MinIO Bronze (every 15 min).

Window (wm_from, wm_to], wm_to = DB NOW() - 10s (so uncommitted rows are not skipped).
The watermark is committed only after every table and the manifest were written, so a failed run is
simply re-read from the same window. Object keys derive from the window START (wm_from): it only moves
forward when a window is committed, so a retry of the same window overwrites its own files, while two
different runs (scheduled vs manual, same data_interval) can never overwrite each other's data.
"""
from datetime import datetime, timedelta, timezone

import pandas as pd
import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
from sqlalchemy import text

from oltp import engine
from s3_utils import put_json, put_parquet

TABLES = ["customers", "orders", "order_items", "order_payments", "order_reviews"]
BRONZE_READY = Dataset("s3://olist-lake/bronze/")
CHUNK = 100_000
LAG_SECONDS = 10
EPOCH = "1970-01-01 00:00:00"


def _stamp(ts: str) -> str:
    """'2026-10-10 09:58:07.519000' -> '20261010T095807519000' (sortable, filesystem safe)."""
    return ts.replace("-", "").replace(":", "").replace(" ", "T").replace(".", "")


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
        wm_to = wm_to.strftime("%Y-%m-%d %H:%M:%S.%f")
        return {t: {"from": Variable.get(f"wm__{t}", EPOCH), "to": wm_to} for t in TABLES}

    @task
    def extract_load(table: str, window: dict):
        w = window[table]
        batch_id = _stamp(w["from"])
        ingest_date = w["to"][:10].replace("-", "")
        sql = text(f"SELECT * FROM {table} WHERE updated_at > :a AND updated_at <= :b ORDER BY updated_at")
        rows, part = 0, 0
        # money columns are read as float64 (coerce_float) to keep the Parquet schema stable across chunks
        with engine().connect().execution_options(stream_results=True) as conn:
            for chunk in pd.read_sql(sql, conn, params={"a": w["from"], "b": w["to"]}, chunksize=CHUNK):
                chunk = chunk.assign(_ingested_at=datetime.now(timezone.utc).replace(tzinfo=None),
                                     _source_table=table, _batch_id=batch_id, _op="upsert")
                key = f"bronze/{table}/ingest_date={ingest_date}/batch_id={batch_id}/part-{part:05d}.parquet"
                put_parquet(chunk, key)
                rows += len(chunk)
                part += 1
        return {"table": table, "rows": rows, "parts": part, "from": w["from"], "to": w["to"]}

    @task
    def write_manifest(results: list, window: dict):
        batch_id = _stamp(window[TABLES[0]]["to"])
        manifest = {"batch_id": batch_id, "tables": list(results),
                    "total_rows": sum(r["rows"] for r in results)}
        put_json(manifest, f"bronze/_manifests/batch_id={batch_id}/manifest.json")
        return manifest["total_rows"]

    @task(outlets=[BRONZE_READY])
    def commit_watermark(window: dict, total_rows: int):
        for t, w in window.items():
            Variable.set(f"wm__{t}", w["to"])
        return total_rows

    window = get_window()
    results = extract_load.partial(window=window).expand(table=TABLES)
    commit_watermark(window, write_manifest(results, window))


ingest_oltp_to_bronze()
