"""Build Silver dimension tables from Bronze (aligned with schema.sql)."""

from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.spark_utils import latest, norm_city, norm_zip, read_bronze, write_table

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
    """Match schema.sql dim_geolocation.zip_code_prefix INT (leading zeros dropped)."""
    return norm_zip(column).cast("int")


def build_dimensions(spark: SparkSession) -> None:
    geo = read_bronze(spark, "olist_geolocation").withColumn(
        "zip_code_prefix", zip_as_int("geolocation_zip_code_prefix")
    )
    geo = geo.filter(F.col("zip_code_prefix").isNotNull())
    geo = geo.withColumn("lat", F.col("geolocation_lat").cast("double")).withColumn(
        "lng", F.col("geolocation_lng").cast("double")
    )
    dim_geo = geo.groupBy("zip_code_prefix").agg(
        F.min(norm_city("geolocation_city")).alias("city"),
        F.upper(F.min(F.trim(F.col("geolocation_state")))).alias("state"),
        F.avg("lat").cast("double").alias("latitude"),
        F.avg("lng").cast("double").alias("longitude"),
        F.count(F.lit(1)).cast("long").alias("source_row_count"),
    )
    dim_geo = dim_geo.select(
        F.xxhash64(F.col("zip_code_prefix").cast("string")).alias("geolocation_key"),
        "zip_code_prefix",
        "city",
        "state",
        "latitude",
        "longitude",
        "source_row_count",
    )
    write_table(dim_geo, "iceberg.silver.dim_geolocation")

    products = latest(read_bronze(spark, "olist_products"), ["product_id"]).filter(
        F.col("product_id").isNotNull()
    )
    # Missing category -> Unknown member (keeps category_key non-null for star joins).
    # Numeric product attributes stay null when absent.
    products = products.withColumn(
        "product_category_name", F.coalesce(F.col("product_category_name"), F.lit("unknown"))
    )
    translation = latest(
        read_bronze(spark, "product_category_name_translation"), ["product_category_name"]
    ).select("product_category_name", "product_category_name_english")
    hardcoded = spark.createDataFrame(
        CATEGORY_EN_HARDCODE,
        ["product_category_name", "product_category_name_english"],
    )
    # Hardcoded rows win over source translation for the same category name.
    translation = translation.join(
        hardcoded.select("product_category_name"), "product_category_name", "left_anti"
    ).unionByName(hardcoded)
    names = products.select("product_category_name").unionByName(
        translation.select("product_category_name")
    )
    names = names.unionByName(
        spark.createDataFrame([("unknown",)], ["product_category_name"])
    ).distinct()
    names = names.filter(F.col("product_category_name").isNotNull())
    dim_category = names.join(translation, "product_category_name", "left").select(
        F.xxhash64("product_category_name").alias("category_key"),
        "product_category_name",
        F.coalesce(F.col("product_category_name_english"), F.col("product_category_name")).alias(
            "product_category_name_english"
        ),
    )
    write_table(dim_category, "iceberg.silver.dim_category")

    geo_keys = dim_geo.select(F.col("zip_code_prefix").alias("geo_zip"), "geolocation_key")
    customers = latest(read_bronze(spark, "olist_customers"), ["customer_id"])
    customers = customers.filter(
        F.col("customer_id").isNotNull() & F.col("customer_unique_id").isNotNull()
    )
    customers = customers.withColumn("zip_code_prefix", zip_as_int("customer_zip_code_prefix"))
    customers = customers.withColumn("city", norm_city("customer_city")).withColumn(
        "state", F.upper(F.trim(F.col("customer_state")))
    )
    customers = customers.withColumn("customer_key", F.xxhash64("customer_unique_id"))
    customers = customers.join(geo_keys, customers.zip_code_prefix == geo_keys.geo_zip, "left")

    person = Window.partitionBy("customer_unique_id").orderBy(F.col("customer_id").desc())
    dim_customer = (
        customers.withColumn("_rn", F.row_number().over(person))
        .filter(F.col("_rn") == 1)
        .select("customer_key", "customer_unique_id", "city", "state", "geolocation_key")
    )
    write_table(dim_customer, "iceberg.silver.dim_customer")

    sellers = latest(read_bronze(spark, "olist_sellers"), ["seller_id"]).filter(
        F.col("seller_id").isNotNull()
    )
    sellers = sellers.withColumn("zip_code_prefix", zip_as_int("seller_zip_code_prefix"))
    sellers = sellers.withColumn("city", norm_city("seller_city")).withColumn(
        "state", F.upper(F.trim(F.col("seller_state")))
    )
    dim_seller = sellers.join(geo_keys, sellers.zip_code_prefix == geo_keys.geo_zip, "left").select(
        F.xxhash64("seller_id").alias("seller_key"),
        "seller_id",
        "city",
        "state",
        "geolocation_key",
    )
    write_table(dim_seller, "iceberg.silver.dim_seller")

    dim_product = products.join(
        dim_category.select("product_category_name", "category_key"), "product_category_name", "left"
    ).select(
        F.xxhash64("product_id").alias("product_key"),
        "product_id",
        "category_key",
        "product_category_name",
        F.col("product_name_lenght").cast("int").alias("product_name_length"),
        F.col("product_description_lenght").cast("int").alias("product_description_length"),
        F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
        F.col("product_weight_g").cast("double").alias("product_weight_g"),
        F.col("product_length_cm").cast("double").alias("product_length_cm"),
        F.col("product_height_cm").cast("double").alias("product_height_cm"),
        F.col("product_width_cm").cast("double").alias("product_width_cm"),
    )
    write_table(dim_product, "iceberg.silver.dim_product")

    purchases = latest(read_bronze(spark, "olist_orders"), ["order_id"]).select(
        F.to_timestamp("order_purchase_timestamp").alias("order_purchase_timestamp")
    )
    bounds = purchases.agg(
        F.min("order_purchase_timestamp").alias("min_ts"),
        F.max("order_purchase_timestamp").alias("max_ts"),
    ).collect()[0]
    start = date(2016, 1, 1)
    end = date(2018, 12, 31)
    if bounds.min_ts is not None:
        start = min(start, bounds.min_ts.date())
    if bounds.max_ts is not None:
        end = max(end, bounds.max_ts.date())
    spine = spark.sql(
        f"SELECT explode(sequence(to_date('{start.isoformat()}'), to_date('{end.isoformat()}'), interval 1 day)) AS date"
    )
    dim_date = spine.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )
    write_table(dim_date, "iceberg.silver.dim_date")
