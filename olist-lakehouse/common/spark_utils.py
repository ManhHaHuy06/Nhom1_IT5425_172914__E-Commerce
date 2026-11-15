"""Shared Spark session, DDL, and dataframe helpers."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def source_lake_conf() -> dict[str, str]:
    """Per-bucket S3A settings for the hosting_server MinIO (a different endpoint than the lakehouse MinIO).

    Hadoop S3A reads ``fs.s3a.bucket.<bucket>.*`` before the global ``fs.s3a.*``, so ``s3a://<bucket>/``
    goes to the source lake while ``s3a://lakehouse`` keeps using spark-defaults.conf.
    Values come from infrastructure/.env (SOURCE_S3_*), passed to the Spark containers by docker compose.
    """
    bucket = os.getenv("SOURCE_S3_BUCKET")
    endpoint = os.getenv("SOURCE_S3_ENDPOINT")
    if not (bucket and endpoint):
        return {}
    prefix = f"spark.hadoop.fs.s3a.bucket.{bucket}"
    conf = {
        f"{prefix}.endpoint": endpoint,
        f"{prefix}.path.style.access": "true",
        f"{prefix}.connection.ssl.enabled": str(endpoint.startswith("https")).lower(),
    }
    if os.getenv("SOURCE_S3_ACCESS_KEY"):
        conf[f"{prefix}.access.key"] = os.environ["SOURCE_S3_ACCESS_KEY"]
        conf[f"{prefix}.secret.key"] = os.getenv("SOURCE_S3_SECRET_KEY", "")
    return conf


def spark_session() -> SparkSession:
    builder = SparkSession.builder.appName("olist-lakehouse")
    for key, value in source_lake_conf().items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def split_sql(text: str) -> list[str]:
    text = text.replace("\ufeff", "")
    lines = []
    for line in text.splitlines():
        if "--" in line:
            line = line[: line.index("--")]
        lines.append(line)
    return [statement.strip() for statement in "\n".join(lines).split(";") if statement.strip()]


def apply_ddl(spark: SparkSession, layers_dir: str) -> None:
    for relative in (
        "bronze/create_bronze_tables.sql",
        "silver/create_silver_tables.sql",
        "gold/create_gold_tables.sql",
    ):
        path = os.path.join(layers_dir, relative)
        print(f"Applying {path}")
        with open(path, encoding="utf-8-sig") as handle:
            for statement in split_sql(handle.read()):
                spark.sql(statement)
    print("DDL complete")


def blank_to_null(df: DataFrame) -> DataFrame:
    for name, dtype in df.dtypes:
        if dtype == "string":
            trimmed = F.trim(F.col(name))
            df = df.withColumn(name, F.when(trimmed == "", None).otherwise(trimmed))
    return df


def latest(df: DataFrame, keys: list[str]) -> DataFrame:
    """Newest version per key: OLTP updated_at first (hosting_server lands every update), then load time."""
    order = [F.col("loaded_at").desc_nulls_last()]
    if "source_updated_at" in df.columns:
        order.insert(0, F.col("source_updated_at").desc_nulls_last())
    window = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


# Applied after lower(); keeps sao paulo == são paulo.
_ACCENT_FROM = "áàâãäåāăąéèêëēĕėęěíìîïĩīĭįıóòôõöøōŏőúùûüũūŭůűųçćčñńņňýÿžźżďđłřşšśťţ"
_ACCENT_TO = "aaaaaaaaaeeeeeeeeeiiiiiiiiiooooooooouuuuuuuuuucccnnnnyyzzzddlrssstt"


def norm_city(column: str) -> F.Column:
    """Lower/trim/collapse spaces and strip accents for city synonym matching."""
    cleaned = F.lower(F.trim(F.regexp_replace(F.col(column), r"\s+", " ")))
    return F.translate(cleaned, _ACCENT_FROM, _ACCENT_TO)


def norm_zip(column: str) -> F.Column:
    digits = F.regexp_replace(F.col(column), r"[^0-9]", "")
    return F.when(F.length(digits) == 0, None).otherwise(F.lpad(digits, 5, "0"))


def write_table(df: DataFrame, table: str) -> None:
    """Replace the whole table: Silver is rebuilt from all of Bronze on every run."""
    cached = df.cache()
    count = cached.count()
    cached.writeTo(table).overwrite(F.lit(True))
    cached.unpersist()
    print(f"{table}: {count} rows")


def read_bronze(spark: SparkSession, table: str) -> DataFrame:
    return blank_to_null(spark.table(f"iceberg.bronze.{table}"))


def latest_bronze(spark: SparkSession, table: str, keys: list[str]) -> DataFrame:
    """Bronze table reduced to its newest version per business key."""
    return latest(read_bronze(spark, table), keys)
