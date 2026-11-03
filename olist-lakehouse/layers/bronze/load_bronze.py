"""Load crawler CSV objects from MinIO into Iceberg Bronze tables."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.spark_utils import blank_to_null

# Crawler handoff layout under s3a://handoff/bronze/ (olist-crawler-to-minio).
# transaction: bronze/{dataset}/date=YYYY-MM-DD/{dataset}.csv
# master:      bronze/{dataset}/version=N/pages/part-*.csv
CRAWLER_SOURCES = {
    "olist_orders": {"kind": "transaction", "dataset": "orders"},
    "olist_order_items": {"kind": "transaction", "dataset": "order_items"},
    "olist_order_payments": {"kind": "transaction", "dataset": "payments"},
    "olist_order_reviews": {"kind": "transaction", "dataset": "reviews"},
    "olist_customers": {"kind": "master", "dataset": "customers"},
    "olist_products": {"kind": "master", "dataset": "products"},
    "olist_sellers": {"kind": "master", "dataset": "sellers"},
    "olist_geolocation": {"kind": "master", "dataset": "geolocation"},
    "product_category_name_translation": {"kind": "master", "dataset": "categories"},
}

METADATA = ("loaded_at", "source_file", "batch_id")
DEFAULT_SOURCE_URI = "s3a://handoff/bronze"


def write_bronze(frame: DataFrame, table: str, batch_id: str, mode: str, source_label: str) -> None:
    full_name = f"iceberg.bronze.{table}"
    if "source_file" not in frame.columns:
        frame = frame.withColumn("source_file", F.lit(source_label))
    frame = (
        frame.withColumn("loaded_at", F.current_timestamp()).withColumn("batch_id", F.lit(batch_id))
    )
    writer = frame.writeTo(full_name)
    if mode == "append":
        writer.append()
    else:
        writer.overwritePartitions()
    print(f"{full_name}: loaded from {source_label} mode={mode} batch={batch_id}")


def crawler_glob(source_uri: str, kind: str, dataset: str, start_date: str | None, end_date: str | None) -> str:
    base = source_uri.rstrip("/")
    if kind == "transaction":
        if start_date and end_date and start_date == end_date:
            return f"{base}/{dataset}/date={start_date}/{dataset}.csv"
        return f"{base}/{dataset}/date=*/{dataset}.csv"
    return f"{base}/{dataset}/version=*/pages/*.csv"


def filter_transaction_dates(frame: DataFrame, start_date: str | None, end_date: str | None) -> DataFrame:
    if not start_date and not end_date:
        return frame
    date_col = F.regexp_extract(F.col("source_file"), r"date=(\d{4}-\d{2}-\d{2})", 1)
    frame = frame.withColumn("_crawl_date", date_col)
    if start_date:
        frame = frame.filter(F.col("_crawl_date") >= F.lit(start_date))
    if end_date:
        frame = frame.filter(F.col("_crawl_date") <= F.lit(end_date))
    return frame.drop("_crawl_date")


def load_bronze_from_crawler(
    spark: SparkSession,
    source_uri: str,
    batch_id: str,
    mode: str,
    start_date: str | None,
    end_date: str | None,
) -> None:
    for table, spec in CRAWLER_SOURCES.items():
        path = crawler_glob(source_uri, spec["kind"], spec["dataset"], start_date, end_date)
        print(f"Reading Bronze handoff: {path}")
        try:
            landed = spark.read.option("header", True).option("inferSchema", False).csv(path)
        except Exception as exc:  # noqa: BLE001 - surface missing prefix clearly
            raise FileNotFoundError(
                f"No crawler objects for {table} at {path}. "
                "Ensure crawler wrote to s3a://handoff/bronze on the shared MinIO bucket."
            ) from exc

        landed = landed.withColumn("source_file", F.input_file_name())
        if spec["kind"] == "transaction":
            landed = filter_transaction_dates(landed, start_date, end_date)
        else:
            # Prefer the newest master snapshot when multiple version=* folders exist.
            version_col = F.regexp_extract(F.col("source_file"), r"version=(\d+)", 1).cast("int")
            landed = landed.withColumn("_master_version", version_col)
            max_version = landed.agg(F.max("_master_version")).collect()[0][0]
            if max_version is not None:
                landed = landed.filter(F.col("_master_version") == max_version)
            landed = landed.drop("_master_version")

        if not landed.head(1):
            raise FileNotFoundError(
                f"Empty crawler dataset for {table} at {path}"
                + (
                    f" (date range [{start_date or '...'}, {end_date or '...'}])"
                    if spec["kind"] == "transaction" and (start_date or end_date)
                    else ""
                )
            )

        expected = [
            field.name
            for field in spark.table(f"iceberg.bronze.{table}").schema.fields
            if field.name not in METADATA
        ]
        data_cols = [c for c in landed.columns if c != "source_file"]
        renamed = [name.replace("\ufeff", "").strip() for name in data_cols]
        frame = landed.select(*data_cols, "source_file").toDF(*renamed, "source_file")
        for column in expected:
            if column not in frame.columns:
                frame = frame.withColumn(column, F.lit(None).cast("string"))
        frame = blank_to_null(frame.select(*expected, "source_file"))
        write_bronze(frame, table, batch_id, mode, path)


def load_bronze(
    spark: SparkSession,
    batch_id: str,
    mode: str,
    source_uri: str = DEFAULT_SOURCE_URI,
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    load_bronze_from_crawler(spark, source_uri, batch_id, mode, start_date, end_date)
