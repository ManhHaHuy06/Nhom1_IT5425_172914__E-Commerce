"""Data-quality quarantine writer for Silver 3NF checks (iceberg.quarantine.silver_rejected_records)."""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

QUARANTINE_TABLE = "iceberg.quarantine.silver_rejected_records"
VALID_TARGETS = frozenset({
    "silver.geolocation",
    "silver.categories",
    "silver.customers",
    "silver.sellers",
    "silver.products",
    "silver.orders",
    "silver.order_items",
    "silver.order_payments",
    "silver.order_reviews",
})
METADATA_COLS = {"source_updated_at", "source_file", "loaded_at", "batch_id"}


def build_rejects(
    df: DataFrame,
    source_table: str,
    target_table: str,
    key_expr: Column,
    rule_code: Column | str,
    reason: Column | str,
    batch_id: str,
    payload_cols: list[str] | None = None,
) -> DataFrame:
    """Project rejected rows into the unified quarantine schema with JSON raw_payload."""
    cols = payload_cols if payload_cols is not None else [c for c in df.columns if c not in METADATA_COLS]
    rule_col = F.lit(rule_code) if isinstance(rule_code, str) else rule_code
    reason_col = F.lit(reason) if isinstance(reason, str) else reason
    src_upd = F.col("source_updated_at").cast("string") if "source_updated_at" in df.columns else F.lit(None).cast("string")
    src_file = F.col("source_file").cast("string") if "source_file" in df.columns else F.lit(None).cast("string")
    return df.select(
        F.current_timestamp().alias("quarantined_at"),
        F.lit(batch_id).cast("string").alias("batch_id"),
        F.lit(source_table).cast("string").alias("source_table"),
        F.lit(target_table).cast("string").alias("target_table"),
        key_expr.cast("string").alias("record_key"),
        rule_col.cast("string").alias("rule_code"),
        reason_col.cast("string").alias("reason"),
        src_upd.alias("source_updated_at"),
        src_file.alias("source_file"),
        F.to_json(F.struct(*[F.col(c) for c in cols])).alias("raw_payload"),
    )


def write_quarantine(spark: SparkSession, reject_dfs: list[DataFrame], target_table: str) -> int:
    """Replace only ``target_table`` partition so Silver full rebuilds never duplicate rejected rows."""
    if target_table not in VALID_TARGETS:
        raise ValueError(f"Unknown quarantine target_table: {target_table}")
    if not reject_dfs:
        spark.sql(f"DELETE FROM {QUARANTINE_TABLE} WHERE target_table = '{target_table}'")
        print(f"{QUARANTINE_TABLE}[{target_table}]: 0 rows")
        return 0
    combined = reduce(lambda a, b: a.unionByName(b), reject_dfs).cache()
    try:
        count = combined.count()
        if count == 0:
            spark.sql(f"DELETE FROM {QUARANTINE_TABLE} WHERE target_table = '{target_table}'")
        else:
            combined.writeTo(QUARANTINE_TABLE).overwrite(F.col("target_table") == F.lit(target_table))
        print(f"{QUARANTINE_TABLE}[{target_table}]: {count} rows")
        return count
    finally:
        combined.unpersist()
