"""Daily full snapshot of reference tables -> MinIO Bronze (bronze/ref_<table>/snapshot_date=YYYY-MM-DD/)."""
from datetime import datetime, timedelta, timezone

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from sqlalchemy import text

from oltp import engine
from s3_utils import put_parquet

REF = {
    "products": "ref_products",
    "sellers": "ref_sellers",
    "geolocation": "ref_geolocation",
    "product_category_name_translation": "ref_category_translation",
}
CHUNK = 200_000


@dag(
    dag_id="ingest_reference_snapshot",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["ingestion", "bronze", "reference"],
)
def ingest_reference_snapshot():
    @task
    def snapshot(table: str, **ctx):
        snap = ctx["ds"]
        rows, part = 0, 0
        with engine().connect().execution_options(stream_results=True) as conn:
            for chunk in pd.read_sql(text(f"SELECT * FROM {table}"), conn, chunksize=CHUNK):
                chunk = chunk.assign(_ingested_at=datetime.now(timezone.utc).replace(tzinfo=None),
                                     _source_table=table, _snapshot_date=snap)
                put_parquet(chunk, f"bronze/{REF[table]}/snapshot_date={snap}/part-{part:05d}.parquet")
                rows += len(chunk)
                part += 1
        return {"table": table, "rows": rows, "parts": part}

    snapshot.expand(table=list(REF))


ingest_reference_snapshot()
