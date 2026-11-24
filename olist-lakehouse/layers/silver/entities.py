"""Build Silver 3NF master entities from Bronze (natural keys, no surrogate keys or aggregations)."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.spark_utils import latest, latest_bronze, norm_city, norm_zip, read_bronze, write_table
from layers.silver.quarantine import build_rejects, write_quarantine

# Categories present in products but missing from product_category_name_translation.
CATEGORY_EN_HARDCODE = [
    ("pc_gamer", "pcs"),
    (
        "portateis_cozinha_e_preparadores_de_alimentos",
        "portable_kitchen_food_preparers",
    ),
    ("unknown", "unknown"),
]


def zip_as_int(column: str) -> F.Column:
    """Normalize Brazilian 5-digit zip prefix to INT (leading zeros dropped for consistent joins)."""
    return norm_zip(column).cast("int")


def _latest_snapshot_rows(df):
    """Filter an append-only snapshot table (no row PK, e.g. geolocation) to its newest snapshot date."""
    max_ts = df.agg(F.max("source_updated_at").alias("m")).first()["m"]
    if max_ts is None:
        return df
    return df.filter(F.col("source_updated_at") == F.lit(max_ts))


def build_entities(spark: SparkSession, batch_id: str) -> None:
    # 1. geolocation (latest Bronze snapshot only so multi-day Bronze history never inflates row counts)
    geo_raw = _latest_snapshot_rows(read_bronze(spark, "olist_geolocation")).withColumn(
        "zip_code_prefix", zip_as_int("geolocation_zip_code_prefix")
    )
    geo_bad = geo_raw.filter(F.col("zip_code_prefix").isNull())
    write_quarantine(
        spark,
        [build_rejects(
            geo_bad, "olist_geolocation", "silver.geolocation",
            F.coalesce(F.col("geolocation_zip_code_prefix"), F.lit("<null>")),
            "INVALID_ZIP", "geolocation_zip_code_prefix is null or non-numeric", batch_id,
            ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"],
        )],
        "silver.geolocation",
    )
    geo = (
        geo_raw.filter(F.col("zip_code_prefix").isNotNull())
        .withColumn("lat", F.col("geolocation_lat").cast("double"))
        .withColumn("lng", F.col("geolocation_lng").cast("double"))
        .groupBy("zip_code_prefix")
        .agg(
            F.min(norm_city("geolocation_city")).alias("city"),
            F.upper(F.min(F.trim(F.col("geolocation_state")))).alias("state"),
            F.avg("lat").cast("double").alias("latitude"),
            F.avg("lng").cast("double").alias("longitude"),
            F.count(F.lit(1)).cast("long").alias("source_row_count"),
        )
    )
    write_table(geo, "iceberg.silver.geolocation")

    # 2. categories & products (quarantine NULL PKs before latest() so multiple NULL rows are never collapsed)
    products_bronze = read_bronze(spark, "olist_products")
    products_bad = products_bronze.filter(F.col("product_id").isNull())
    write_quarantine(
        spark,
        [build_rejects(
            products_bad, "olist_products", "silver.products",
            F.lit("<null>"), "NULL_PK", "product_id is null", batch_id,
            ["product_id", "product_category_name"],
        )],
        "silver.products",
    )
    products = latest(products_bronze.filter(F.col("product_id").isNotNull()), ["product_id"]).withColumn(
        "product_category_name", F.coalesce(F.col("product_category_name"), F.lit("unknown"))
    )

    translation = latest_bronze(
        spark, "product_category_name_translation", ["product_category_name"]
    ).select("product_category_name", "product_category_name_english")
    hardcoded = spark.createDataFrame(
        CATEGORY_EN_HARDCODE,
        ["product_category_name", "product_category_name_english"],
    )
    # Hardcoded rows win over source translation for the same category name.
    translation = translation.join(
        hardcoded.select("product_category_name"), "product_category_name", "left_anti"
    ).unionByName(hardcoded)

    names = (
        products.select("product_category_name")
        .unionByName(translation.select("product_category_name"))
        .unionByName(spark.createDataFrame([("unknown",)], ["product_category_name"]))
        .distinct()
        .filter(F.col("product_category_name").isNotNull())
    )
    categories = names.join(translation, "product_category_name", "left").select(
        "product_category_name",
        F.coalesce(F.col("product_category_name_english"), F.col("product_category_name")).alias(
            "product_category_name_english"
        ),
    )
    write_table(categories, "iceberg.silver.categories")

    # 3. customers (quarantine NULL PKs before latest(); 1 row per customer_unique_id from max customer_id)
    customers_bronze = read_bronze(spark, "olist_customers")
    customers_bad = customers_bronze.filter(
        F.col("customer_id").isNull() | F.col("customer_unique_id").isNull()
    )
    write_quarantine(
        spark,
        [build_rejects(
            customers_bad, "olist_customers", "silver.customers",
            F.coalesce(F.col("customer_id"), F.col("customer_unique_id"), F.lit("<null>")),
            "NULL_PK", "customer_id or customer_unique_id is null", batch_id,
            ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
        )],
        "silver.customers",
    )
    person = Window.partitionBy("customer_unique_id").orderBy(F.col("customer_id").desc())
    customers = (
        latest(
            customers_bronze.filter(F.col("customer_id").isNotNull() & F.col("customer_unique_id").isNotNull()),
            ["customer_id"],
        )
        .withColumn("zip_code_prefix", zip_as_int("customer_zip_code_prefix"))
        .withColumn("city", norm_city("customer_city"))
        .withColumn("state", F.upper(F.trim(F.col("customer_state"))))
        .withColumn("_rn", F.row_number().over(person))
        .filter(F.col("_rn") == 1)
        .select("customer_unique_id", "zip_code_prefix", "city", "state")
    )
    write_table(customers, "iceberg.silver.customers")

    # 4. sellers
    sellers_bronze = read_bronze(spark, "olist_sellers")
    sellers_bad = sellers_bronze.filter(F.col("seller_id").isNull())
    write_quarantine(
        spark,
        [build_rejects(
            sellers_bad, "olist_sellers", "silver.sellers",
            F.lit("<null>"), "NULL_PK", "seller_id is null", batch_id,
            ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
        )],
        "silver.sellers",
    )
    sellers = (
        latest(sellers_bronze.filter(F.col("seller_id").isNotNull()), ["seller_id"])
        .withColumn("zip_code_prefix", zip_as_int("seller_zip_code_prefix"))
        .withColumn("city", norm_city("seller_city"))
        .withColumn("state", F.upper(F.trim(F.col("seller_state"))))
        .select("seller_id", "zip_code_prefix", "city", "state")
    )
    write_table(sellers, "iceberg.silver.sellers")

    # 5. products
    silver_products = products.select(
        "product_id",
        "product_category_name",
        F.col("product_name_lenght").cast("int").alias("product_name_length"),
        F.col("product_description_lenght").cast("int").alias("product_description_length"),
        F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
        F.col("product_weight_g").cast("double").alias("product_weight_g"),
        F.col("product_length_cm").cast("double").alias("product_length_cm"),
        F.col("product_height_cm").cast("double").alias("product_height_cm"),
        F.col("product_width_cm").cast("double").alias("product_width_cm"),
    )
    write_table(silver_products, "iceberg.silver.products")
