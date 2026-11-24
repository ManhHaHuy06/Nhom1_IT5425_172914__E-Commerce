"""Daily full snapshot of reference tables -> MinIO Landing Zone (landing/ref_<table>/snapshot_date=YYYY-MM-DD/)."""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from landing import land, snapshot_prefix

# OLTP table -> Landing dataset name
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
    tags=["ingestion", "landing", "reference"],
)
def ingest_reference_snapshot():
    @task
    def snapshot(table: str, **ctx):
        snap = ctx["ds"]
        return land(
            table,
            f"SELECT * FROM {table}",
            snapshot_prefix(REF[table], snap),
            meta={"_source_table": table, "_snapshot_date": snap},
            chunksize=CHUNK,
        )

    snapshot.expand(table=list(REF))


ingest_reference_snapshot()
