"""Chunked MySQL extraction to local Parquet, followed by MinIO staging uploads."""
from __future__ import annotations
from s3_utils import bucket
import hashlib
import json
import os
import re
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text

from oltp import engine
from s3_utils import bucket, clear_staging_prefix, put_json, upload_file, verify_bucket

CHUNK_SIZE = int(os.getenv("INGESTION_CHUNK_SIZE", "100000"))
EPOCH = "1970-01-01 00:00:00.000000"
TRANSACTIONAL = {
    "customers": ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
    "orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"],
    "order_items": ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"],
    "order_payments": ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
    "order_reviews": ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date", "review_answer_timestamp"],
}
REFERENCE = {
    "products": ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"],
    "sellers": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    "geolocation": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"],
    "product_category_name_translation": ["product_category_name", "product_category_name_english"],
}


def safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id)[:160]


def make_batch_id(windows: dict) -> str:
    raw = json.dumps(windows, sort_keys=True, separators=(",", ":"))
    return "txn_" + hashlib.sha256(raw.encode()).hexdigest()[:24]


def _write_query_to_parquet(conn, query, params, output_dir: Path, columns: list[str]):
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = parts = 0
    for chunk in pd.read_sql(text(query), conn, params=params, chunksize=CHUNK_SIZE, coerce_float=False):
        # Bronze business fields are STRING. Convert before writing so all Parquet chunks
        # have a stable schema, including chunks containing only NULL values.
        chunk = chunk.astype("string")
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        path = output_dir / f"part-{parts:05d}.parquet"
        pq.write_table(table, path, compression="snappy", coerce_timestamps="us", allow_truncated_timestamps=True)
        rows += len(chunk)
        parts += 1
    if parts == 0:
        # Keep an empty typed Parquet part so the Spark adapter can read every expected table
        # even when a transactional table had no changes in this watermark window.
        empty = pd.DataFrame({column: pd.Series(dtype="string") for column in columns})
        pq.write_table(pa.Table.from_pandas(empty, preserve_index=False), output_dir / "part-00000.parquet", compression="snappy")
        parts = 1
    return {"row_count": rows, "part_count": parts}


def extract_transactions(plan: dict, work_root: str, run_id: str) -> dict:
    batch_id = plan["transaction_batch_id"]
    run_dir = Path(work_root) / safe_run_id(run_id) / "transactions"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    results = {}
    with engine().connect().execution_options(stream_results=True) as conn:
        for table, columns in TRANSACTIONAL.items():
            w = plan["transaction_windows"][table]
            cols = ", ".join(f"`{c}`" for c in columns)
            query = f"SELECT {cols} FROM `{table}` WHERE `updated_at` > :wm_from AND `updated_at` <= :wm_to ORDER BY `updated_at`"
            results[table] = _write_query_to_parquet(
                conn, query, {"wm_from": w["from"], "wm_to": w["to"]}, run_dir / table, columns
            )
            print(f"[EXTRACT] {table}: {results[table]['row_count']:,} rows")
    return {"mode": "transactional", "batch_id": batch_id, "work_dir": str(run_dir), "tables": results,
            "total_rows": sum(x["row_count"] for x in results.values()), "wm_to": plan["wm_to"]}


def extract_reference(plan: dict, work_root: str, run_id: str) -> dict:
    if not plan["reference_due"]:
        return {"mode": "reference", "due": False, "total_rows": 0, "tables": {}}
    snapshot_id = plan["snapshot_id"]
    run_dir = Path(work_root) / safe_run_id(run_id) / "reference"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    results = {}
    with engine().connect().execution_options(stream_results=True) as conn:
        for table, columns in REFERENCE.items():
            cols = ", ".join(f"`{c}`" for c in columns)
            results[table] = _write_query_to_parquet(
                conn, f"SELECT {cols} FROM `{table}`", {}, run_dir / table, columns
            )
            if results[table]["row_count"] == 0:
                raise RuntimeError(f"Reference table {table} is empty; refusing to commit snapshot {snapshot_id}")
            print(f"[SNAPSHOT] {table}: {results[table]['row_count']:,} rows")
    # The existing Lakehouse dimensions.py aggregates every Bronze geolocation row and does not
    # select a latest snapshot. Re-appending daily snapshots would inflate source_row_count.
    # Fingerprint the source so an unexpected change stops safely instead of being appended.
    # Order-independent multiset fingerprint (geolocation has no primary key and MySQL
    # does not promise SELECT row order). Sum + XOR + row count also preserve duplicates.
    geo_count = 0
    geo_sum = 0
    geo_xor = 0
    mask64 = (1 << 64) - 1
    for parquet_path in sorted((run_dir / "geolocation").glob("*.parquet")):
        frame = pd.read_parquet(parquet_path)
        hashes = pd.util.hash_pandas_object(frame, index=False).to_numpy(dtype="uint64")
        geo_count += int(len(hashes))
        geo_sum = (geo_sum + sum(int(value) for value in hashes)) & mask64
        if len(hashes):
            chunk_xor = 0
            for value in hashes:
                chunk_xor ^= int(value)
            geo_xor ^= chunk_xor
    geolocation_sha256 = f"{geo_count}:{geo_sum:016x}:{geo_xor:016x}"
    previous_geo = plan.get("previous_geolocation_sha256", "")
    if previous_geo and previous_geo != geolocation_sha256:
        raise RuntimeError(
            "Geolocation source content changed. The copied Lakehouse dimensions.py aggregates all "
            "Bronze geolocation rows without latest-snapshot filtering. Refusing to append a new "
            "snapshot because it would mix old and new geolocation records. A controlled Bronze "
            "replacement/rebuild policy must be approved before this source is changed."
        )
    return {"mode": "reference", "due": True, "snapshot_id": snapshot_id, "work_dir": str(run_dir),
            "tables": results, "total_rows": sum(x["row_count"] for x in results.values()),
            "geolocation_sha256": geolocation_sha256, "geolocation_already_loaded": bool(previous_geo)}


def stage_batch(result: dict, prefix: str) -> dict:
    if result.get("mode") == "reference" and not result.get("due"):
        return {**result, "staged": False}
    verify_bucket()
    # A task retry may produce fewer Parquet parts than a failed previous attempt.
    # Clear only the exact staging prefix so Spark cannot read stale extra part files.
    clear_staging_prefix(prefix)
    batch_root = f"{prefix.rstrip('/')}/"
    for table, details in result["tables"].items():
        local_table_dir = Path(result["work_dir"]) / table
        for path in sorted(local_table_dir.glob("*.parquet")):
            upload_file(str(path), f"{batch_root}{table}/{path.name}")
    manifest = {
        "batch_id": result.get("batch_id") or result.get("snapshot_id"),
        "mode": result["mode"],
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "total_rows": result["total_rows"],
        "tables": result["tables"],
        "status": "SUCCESS",
    }
    put_json(manifest, f"{batch_root}manifest.json")
    return {
        **result,
        "staged": True,
        "source_uri": f"s3a://{bucket()}/{prefix.strip('/')}"
    }
