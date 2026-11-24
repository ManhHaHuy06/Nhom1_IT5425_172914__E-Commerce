"""Landing zone layout contract (read by olist-lakehouse layers/bronze/load_bronze.py) and shared landing loop.

    landing/<table>/ingest_date=YYYYMMDD/batch_id=<wm_from stamp>/part-NNNNN.parquet   incremental (every row version)
    landing/ref_<table>/snapshot_date=YYYY-MM-DD/part-NNNNN.parquet                    daily full snapshot
    landing/_manifests/batch_id=<wm_to stamp>/manifest.json                            one per incremental run

Every row gets ``_ingested_at`` (UTC) plus the string metadata columns passed to ``land``.
"""
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
from sqlalchemy import text

from oltp import engine
from s3_utils import arrow_schema, delete_keys, list_keys, put_parquet

BUCKET_PREFIX = "landing"
# Incremental (watermark) tables, shared by ingest_oltp_to_landing and reconcile_landing
TRANSACTION_TABLES = ["customers", "orders", "order_items", "order_payments", "order_reviews"]


def watermark_var(table: str) -> str:
    """Airflow Variable holding the committed watermark (max landed ``updated_at``) of ``table``."""
    return f"wm__{table}"


def stamp(ts: str) -> str:
    """'2026-10-10 09:58:07.519000' -> '20261010T095807519000' (sortable, filesystem safe)."""
    return ts.replace("-", "").replace(":", "").replace(" ", "T").replace(".", "")


def incremental_prefix(table: str, ingest_date: str, batch_id: str) -> str:
    return f"{BUCKET_PREFIX}/{table}/ingest_date={ingest_date}/batch_id={batch_id}"


def snapshot_prefix(ref_table: str, snapshot_date: str) -> str:
    return f"{BUCKET_PREFIX}/{ref_table}/snapshot_date={snapshot_date}"


def manifest_key(batch_id: str) -> str:
    return f"{BUCKET_PREFIX}/_manifests/batch_id={batch_id}/manifest.json"


def land(table: str, sql: str, prefix: str, meta: dict, chunksize: int, params: dict | None = None) -> dict:
    """Stream ``sql`` (over OLTP ``table``) to ``<prefix>/part-NNNNN.parquet`` in chunks.

    The Parquet schema is fixed: OLTP columns of ``table`` + ``_ingested_at`` + ``meta`` keys (strings, in order).
    Part numbers are deterministic, so a retry of the same query overwrites its own files. After all parts are
    written, objects left in ``prefix`` by an earlier attempt that produced more parts are deleted (otherwise they
    would be read as extra rows). New parts are written first, so a reader never sees the folder empty.
    """
    eng = engine()
    schema = arrow_schema(eng, table, [pa.field("_ingested_at", pa.timestamp("us"))]
                          + [pa.field(k, pa.string()) for k in meta])
    rows = part = 0
    written = set()
    # money columns are read as float64 (coerce_float) to keep the Parquet schema stable across chunks
    with eng.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql(text(sql), conn, params=params, chunksize=chunksize):
            chunk = chunk.assign(_ingested_at=datetime.now(timezone.utc).replace(tzinfo=None), **meta)
            key = f"{prefix}/part-{part:05d}.parquet"
            put_parquet(chunk, key, schema)
            written.add(key)
            rows += len(chunk)
            part += 1
    stale = [k for k in list_keys(f"{prefix}/") if k not in written]
    delete_keys(stale)
    return {"table": table, "rows": rows, "parts": part, "stale_parts_removed": len(stale)}
