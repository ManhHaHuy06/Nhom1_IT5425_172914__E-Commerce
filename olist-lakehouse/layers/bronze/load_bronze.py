"""Load the hosting_server Landing Zone (Parquet on its MinIO) into Iceberg ``iceberg.bronze.*``.

Source layout (``s3a://olist-lake/landing``, written by hosting_server/airflow, endpoint/credentials from
``SOURCE_S3_*``, see ``common.spark_utils.spark_session``):

    incremental: {dataset}/ingest_date=YYYYMMDD/batch_id=<wm_from>/part-*.parquet
                 every committed version of a row (one per OLTP update), with ``updated_at``
    snapshot:    ref_{dataset}/snapshot_date=YYYY-MM-DD/part-*.parquet (daily full copy; all snapshots kept)

Bronze is append-only by default (idempotent on ``source_file`` + ``source_file_mtime``) and keeps every landed row version as STRING
in the original CSV text form. OLTP ``updated_at`` (or ``snapshot_date`` for reference tables) is stored in
``source_updated_at`` so Silver can deterministically pick the latest version.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.spark_utils import blank_to_null

DEFAULT_SOURCE_URI = "s3a://olist-lake/landing"

# Iceberg Bronze table -> (kind, hosting_server landing dataset)
SOURCES = {
    "olist_orders": ("incremental", "orders"),
    "olist_order_items": ("incremental", "order_items"),
    "olist_order_payments": ("incremental", "order_payments"),
    "olist_order_reviews": ("incremental", "order_reviews"),
    "olist_customers": ("incremental", "customers"),
    "olist_products": ("snapshot", "ref_products"),
    "olist_sellers": ("snapshot", "ref_sellers"),
    "olist_geolocation": ("snapshot", "ref_geolocation"),
    "product_category_name_translation": ("snapshot", "ref_category_translation"),
}
MISSING_HINT = {
    "incremental": "Run Airflow DAG ingest_oltp_to_landing at least once.",
    "snapshot": "Run Airflow DAG ingest_reference_snapshot at least once.",
}

METADATA = ("loaded_at", "source_file", "batch_id", "source_updated_at", "source_file_mtime")
# Same text form as the original CSVs, so Silver's to_timestamp()/cast() behave identically.
BUSINESS_TS_FORMAT = "yyyy-MM-dd HH:mm:ss"
# Fixed width with milliseconds: sorts correctly as a string (OLTP updated_at is TIMESTAMP(3)).
VERSION_TS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"


def ensure_metadata_columns(spark: SparkSession, table: str) -> None:
    """Add columns introduced after a table was first created (CREATE TABLE IF NOT EXISTS skips them)."""
    full_name = f"iceberg.bronze.{table}"
    columns = spark.table(full_name).columns
    if "source_updated_at" not in columns:
        spark.sql(f"ALTER TABLE {full_name} ADD COLUMNS (source_updated_at STRING)")
    if "source_file_mtime" not in columns:
        spark.sql(f"ALTER TABLE {full_name} ADD COLUMNS (source_file_mtime TIMESTAMP)")


def data_columns(spark: SparkSession, table: str) -> list[str]:
    return [c for c in spark.table(f"iceberg.bronze.{table}").columns if c not in METADATA]


def source_glob(source_uri: str, kind: str, dataset: str) -> str:
    base = f"{source_uri.rstrip('/')}/{dataset}"
    return f"{base}/ingest_date=*/batch_id=*/*.parquet" if kind == "incremental" else f"{base}/snapshot_date=*/*.parquet"


def as_bronze_string(name: str, dtype: T.DataType, ts_format: str = BUSINESS_TS_FORMAT) -> Column:
    """Typed Parquet column -> Bronze STRING in the CSV text form."""
    column = F.col(name)
    if isinstance(dtype, (T.TimestampType, T.TimestampNTZType, T.DateType)):
        return F.date_format(column, ts_format)
    return column.cast("string")


def filter_ingest_dates(frame: DataFrame, start_date: str | None, end_date: str | None) -> DataFrame:
    """--start-date/--end-date (YYYY-MM-DD) select by ingest_date (when Airflow landed the batch)."""
    if not (start_date or end_date):
        return frame
    ingest_date = F.regexp_extract("source_file", r"ingest_date=(\d{8})", 1)
    if start_date:
        frame = frame.filter(ingest_date >= F.lit(start_date.replace("-", "")))
    if end_date:
        frame = frame.filter(ingest_date <= F.lit(end_date.replace("-", "")))
    return frame


def filter_unloaded_files(spark: SparkSession, table: str, frame: DataFrame, kind: str) -> DataFrame:
    """Drop landing files already in Bronze; a rewritten incremental file (newer mtime) is loaded again.

    An ingest retry keeps batch_id=stamp(wm_from) and rewrites the same part files with a wider window, so the
    path alone would hide the new rows. Rows loaded before source_file_mtime existed (NULL) count as loaded.
    Snapshot files match on path only: a rewritten snapshot would duplicate geolocation rows, which have no key.
    """
    loaded = (spark.table(f"iceberg.bronze.{table}")
              .select("source_file", "source_file_mtime").distinct().alias("loaded"))
    same_file = F.col("landed.source_file") == F.col("loaded.source_file")
    if kind == "incremental":
        same_file &= (F.col("loaded.source_file_mtime").isNull()
                      | (F.col("loaded.source_file_mtime") == F.col("landed.source_file_mtime")))
    return frame.alias("landed").join(loaded, same_file, "left_anti")


def read_landed(spark: SparkSession, path: str, kind: str, table: str) -> DataFrame:
    try:
        return (spark.read.parquet(path)
                .withColumn("source_file", F.input_file_name())
                .withColumn("source_file_mtime", F.col("_metadata.file_modification_time")))
    except Exception as exc:  # noqa: BLE001 - surface a missing prefix clearly
        raise FileNotFoundError(f"No hosting_server objects for {table} at {path}. {MISSING_HINT[kind]}") from exc


def to_bronze_frame(landed: DataFrame, columns: list[str], kind: str) -> DataFrame:
    """Bronze columns (STRING) + source_updated_at + source_file; missing source columns become NULL."""
    dtypes = {f.name: f.dataType for f in landed.schema.fields}
    selected = [as_bronze_string(c, dtypes[c]).alias(c) if c in dtypes else F.lit(None).cast("string").alias(c)
                for c in columns]
    if "updated_at" in dtypes:
        version = as_bronze_string("updated_at", dtypes["updated_at"], VERSION_TS_FORMAT)
    elif kind == "snapshot":
        snap = F.regexp_extract("source_file", r"snapshot_date=(\d{4}-\d{2}-\d{2})", 1)
        version = F.when(snap != "", F.concat(snap, F.lit(" 00:00:00.000")))
    else:
        version = F.lit(None).cast("string")
    return blank_to_null(landed.select(*selected, version.alias("source_updated_at"), "source_file",
                                       "source_file_mtime"))


def write_bronze(frame: DataFrame, table: str, batch_id: str, mode: str) -> None:
    full_name = f"iceberg.bronze.{table}"
    cached = frame.withColumn("loaded_at", F.current_timestamp()).withColumn("batch_id", F.lit(batch_id)).cache()
    try:
        rows = cached.count()
        writer = cached.writeTo(full_name)
        if mode == "append":
            writer.append()
        else:
            writer.overwritePartitions()
        print(f"{full_name}: loaded {rows} rows mode={mode} batch={batch_id}")
    finally:
        cached.unpersist()


def load_bronze(
    spark: SparkSession,
    batch_id: str,
    mode: str = "append",
    source_uri: str = DEFAULT_SOURCE_URI,
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    for table, (kind, dataset) in SOURCES.items():
        ensure_metadata_columns(spark, table)
        path = source_glob(source_uri, kind, dataset)
        print(f"Reading hosting_server Landing: {path}")
        landed = read_landed(spark, path, kind, table)
        if kind == "incremental":
            landed = filter_ingest_dates(landed, start_date, end_date)
        if mode == "append":
            landed = filter_unloaded_files(spark, table, landed, kind)
            if landed.isEmpty():
                if spark.table(f"iceberg.bronze.{table}").isEmpty():
                    window = (f" (ingest_date range [{start_date or '...'}, {end_date or '...'}])"
                              if kind == "incremental" and (start_date or end_date) else "")
                    raise FileNotFoundError(f"Empty hosting_server dataset for {table} at {path}{window}")
                print(f"iceberg.bronze.{table}: 0 new rows (all landing files already loaded)")
                continue
        elif landed.isEmpty():
            window = (f" (ingest_date range [{start_date or '...'}, {end_date or '...'}])"
                      if kind == "incremental" and (start_date or end_date) else "")
            raise FileNotFoundError(f"Empty hosting_server dataset for {table} at {path}{window}")
        write_bronze(to_bronze_frame(landed, data_columns(spark, table), kind), table, batch_id, mode)
