"""Append Integration OLTP Parquet batches into the existing Iceberg Bronze tables.

The target schemas are copied verbatim from the Lakehouse Bronze DDL. This adapter
only replaces the old CSV handoff ingestion path; it never creates or alters tables.
"""
from __future__ import annotations

import argparse
from typing import Optional, Set

from pyspark.sql import SparkSession, functions as F

# Source MySQL table -> (existing Iceberg Bronze table, exact business projection).
TABLES = {
    "customers": ("olist_customers", [
        "customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state",
    ]),
    "orders": ("olist_orders", [
        "order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date",
    ]),
    "order_items": ("olist_order_items", [
        "order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value",
    ]),
    "order_payments": ("olist_order_payments", [
        "order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value",
    ]),
    "order_reviews": ("olist_order_reviews", [
        "review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
        "review_creation_date", "review_answer_timestamp",
    ]),
    "products": ("olist_products", [
        "product_id", "product_category_name", "product_name_lenght", "product_description_lenght",
        "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
    ]),
    "sellers": ("olist_sellers", [
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state",
    ]),
    "geolocation": ("olist_geolocation", [
        "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state",
    ]),
    "product_category_name_translation": ("product_category_name_translation", [
        "product_category_name", "product_category_name_english",
    ]),
}
TRANSACTIONAL = {"customers", "orders", "order_items", "order_payments", "order_reviews"}
REFERENCE = set(TABLES) - TRANSACTIONAL


def _already_loaded(spark: SparkSession, target: str, batch_id: str) -> bool:
    # Existing Bronze tables must already have been initialized by the Lakehouse.
    escaped = batch_id.replace("'", "''")
    return bool(spark.sql(
        f"SELECT 1 FROM iceberg.bronze.{target} WHERE batch_id = '{escaped}' LIMIT 1"
    ).take(1))


def load(spark: SparkSession, mode: str, source_uri: str, batch_id: str, skip_tables: Optional[Set[str]] = None) -> None:
    expected = TRANSACTIONAL if mode == "transactional" else REFERENCE
    skip_tables = skip_tables or set()
    invalid_skips = skip_tables - expected
    if invalid_skips:
        raise ValueError(f"Cannot skip tables outside {mode} mode: {sorted(invalid_skips)}")
    for source_table in sorted(expected):
        if source_table in skip_tables:
            print(f"[BRONZE] SKIP unchanged snapshot table={source_table}", flush=True)
            continue
        target, business_columns = TABLES[source_table]
        path = f"{source_uri.rstrip('/')}/{source_table}"
        print(f"[BRONZE] source={source_table} target=iceberg.bronze.{target} path={path}", flush=True)
        try:
            source = spark.read.parquet(path)
        except Exception as exc:
            raise RuntimeError(f"Cannot read staged Parquet for {source_table}: {path}") from exc

        missing = [c for c in business_columns if c not in source.columns]
        if missing:
            raise RuntimeError(f"{source_table}: missing required source columns: {missing}")
        if _already_loaded(spark, target, batch_id):
            print(f"[BRONZE] SKIP duplicate batch={batch_id} table={target}", flush=True)
            continue

        projected = source.select(*[
            F.col(c).cast("string").alias(c) for c in business_columns
        ])
        if source_table == "geolocation":
            # The Lakehouse may already contain a manually initialized geolocation snapshot.
            # Its existing Silver transformation aggregates every Bronze row (no latest-snapshot
            # key). Append only when Bronze is empty; skip identical or different populated-source
            # snapshots so we never mix generations or inflate source_row_count. A changed
            # geolocation source requires an explicitly approved controlled rebuild.
            existing = spark.table(f"iceberg.bronze.{target}").select(*[
                F.col(c).cast("string").alias(c) for c in business_columns
            ])
            existing_count = existing.count()
            incoming_count = projected.count()
            if existing_count > 0:
                if existing_count == incoming_count:
                    existing_only = existing.exceptAll(projected).limit(1).count()
                    incoming_only = projected.exceptAll(existing).limit(1).count()
                    if existing_only == 0 and incoming_only == 0:
                        print("[BRONZE] SKIP geolocation: incoming snapshot exactly matches existing Lakehouse Bronze", flush=True)
                        continue
                print(
                    "[BRONZE] SKIP geolocation snapshot: existing Lakehouse Bronze is populated and "
                    "the incoming source differs. The current Silver dimensions.py aggregates every "
                    "Bronze geolocation row without latest-snapshot filtering, so appending would mix "
                    "snapshots and distort source_row_count/coordinates. Existing Lakehouse geolocation "
                    "is preserved; changing it requires a separately approved controlled rebuild.",
                    flush=True,
                )
                continue
        output = projected.withColumn("loaded_at", F.current_timestamp()) \
            .withColumn("source_file", F.input_file_name()) \
            .withColumn("batch_id", F.lit(batch_id))
        count = output.count()
        if count == 0:
            print(f"[BRONZE] EMPTY table={target}; nothing appended", flush=True)
            continue
        # Append only. No DDL, overwrite, or changes to existing table definitions.
        output.writeTo(f"iceberg.bronze.{target}").append()
        print(f"[BRONZE] APPENDED table={target} rows={count} batch={batch_id}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["transactional", "reference"], required=True)
    parser.add_argument("--source-uri", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--skip-tables", default="", help="Comma-separated source tables to skip after fingerprint validation")
    args = parser.parse_args()
    spark = SparkSession.builder.appName(f"olist-bronze-{args.mode}-{args.batch_id}").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        load(spark, args.mode, args.source_uri, args.batch_id, set(filter(None, args.skip_tables.split(","))))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
