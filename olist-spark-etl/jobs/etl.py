from __future__ import annotations

import os

import sys

from pyspark.sql import DataFrame, SparkSession

from pyspark.sql import functions as F

from pyspark.sql.types import *

sys.path.append("/opt/olist")

from config.settings import *

# ============================================================

# PATHS

# ============================================================

RAW = f"{S3A_ROOT}/{INPUT_PREFIX}"

CURATED = f"{S3A_ROOT}/{OUTPUT_PREFIX}"

QUARANTINE = f"{S3A_ROOT}/{QUARANTINE_PREFIX}"

# ============================================================

# SCHEMAS

# ============================================================

ORDERS = StructType([

    StructField("order_id", StringType()),

    StructField("customer_id", StringType()),

    StructField("order_status", StringType()),

    StructField("order_purchase_timestamp", TimestampType()),

    StructField("order_approved_at", TimestampType()),

    StructField("order_delivered_carrier_date", TimestampType()),

    StructField("order_delivered_customer_date", TimestampType()),

    StructField("order_estimated_delivery_date", TimestampType()),

])

ITEMS = StructType([

    StructField("order_id", StringType()),

    StructField("order_item_id", IntegerType()),

    StructField("product_id", StringType()),

    StructField("seller_id", StringType()),

    StructField("shipping_limit_date", TimestampType()),

    StructField("price", DoubleType()),

    StructField("freight_value", DoubleType()),

])

PAYMENTS = StructType([

    StructField("order_id", StringType()),

    StructField("payment_sequential", IntegerType()),

    StructField("payment_type", StringType()),

    StructField("payment_installments", IntegerType()),

    StructField("payment_value", DoubleType()),

])

REVIEWS = StructType([

    StructField("review_id", StringType()),

    StructField("order_id", StringType()),

    StructField("review_score", IntegerType()),

    StructField("review_comment_title", StringType()),

    StructField("review_comment_message", StringType()),

    StructField("review_creation_date", TimestampType()),

    StructField("review_answer_timestamp", TimestampType()),

])

CUSTOMERS = StructType([

    StructField("customer_id", StringType()),

    StructField("customer_unique_id", StringType()),

    StructField("customer_zip_code_prefix", IntegerType()),

    StructField("customer_city", StringType()),

    StructField("customer_state", StringType()),

])

SELLERS = StructType([

    StructField("seller_id", StringType()),

    StructField("seller_zip_code_prefix", IntegerType()),

    StructField("seller_city", StringType()),

    StructField("seller_state", StringType()),

])

PRODUCTS = StructType([

    StructField("product_id", StringType()),

    StructField("product_category_name", StringType()),

    StructField("product_name_lenght", IntegerType()),

    StructField("product_description_lenght", IntegerType()),

    StructField("product_photos_qty", IntegerType()),

    StructField("product_weight_g", DoubleType()),

    StructField("product_length_cm", DoubleType()),

    StructField("product_height_cm", DoubleType()),

    StructField("product_width_cm", DoubleType()),

])

CATEGORIES = StructType([

    StructField("product_category_name", StringType()),

    StructField("product_category_name_english", StringType()),

])

GEO = StructType([

    StructField("geolocation_zip_code_prefix", IntegerType()),

    StructField("geolocation_lat", DoubleType()),

    StructField("geolocation_lng", DoubleType()),

    StructField("geolocation_city", StringType()),

    StructField("geolocation_state", StringType()),

])

# ============================================================

# SPARK SESSION

# ============================================================

def spark_session() -> SparkSession:

    return (

        SparkSession.builder

        .appName("Olist-Spark-ETL")

        .master(os.getenv("SPARK_MASTER", "local[*]"))

        .config(

            "spark.sql.shuffle.partitions",

            os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"),

        )

        .config("spark.hadoop.fs.s3a.endpoint", S3A_ENDPOINT)

        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)

        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)

        .config("spark.hadoop.fs.s3a.path.style.access", "true")

        .config(

            "spark.hadoop.fs.s3a.connection.ssl.enabled",

            str(MINIO_SECURE).lower(),

        )

        .config(

            "spark.hadoop.fs.s3a.impl",

            "org.apache.hadoop.fs.s3a.S3AFileSystem",

        )

        .config(

            "spark.hadoop.fs.s3a.aws.credentials.provider",

            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

        )

        .getOrCreate()

    )

# ============================================================

# READ RAW

# ============================================================

def read(

    spark: SparkSession,

    path: str,

    schema: StructType,

) -> DataFrame:

    """

    Read only CSV files from RAW.

    recursiveFileLookup is required because:

        raw/orders/date=...

        raw/products/version=.../pages/...

    pathGlobFilter prevents Spark from accidentally reading

    changes.jsonl or other non-CSV files.

    """

    return (

        spark.read

        .option("header", True)

        .option("recursiveFileLookup", True)

        .option("pathGlobFilter", "*.csv")

        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")

        .schema(schema)

        .csv(f"{RAW}/{path}")

    )

# ============================================================

# STRING CLEANING

# ============================================================

def strings(df: DataFrame) -> DataFrame:

    """

    Trim string columns and convert empty strings to NULL.

    """

    for field in df.schema.fields:

        if isinstance(field.dataType, StringType):

            df = df.withColumn(

                field.name,

                F.when(

                    F.trim(F.col(field.name)) == "",

                    F.lit(None),

                ).otherwise(

                    F.trim(F.col(field.name))

                ),

            )

    return df

# ============================================================

def normalize_zip(col):

    """Normalize ZIP prefixes to a canonical 5-digit string."""

    return F.lpad(F.trim(col.cast("string")), 5, "0")

# WRITE CURATED

# ============================================================

BIGINT_COLUMNS = {

    "dim_geolocation": ["geolocation_key", "source_row_count"],

    "dim_category": ["category_key"],

    "dim_customer": ["customer_key", "geolocation_key"],

    "dim_seller": ["seller_key", "geolocation_key"],

    "dim_product": ["product_key", "category_key"],

    "dim_date": [],

    "fact_orders": ["order_key", "customer_key"],

    "fact_order_items": ["item_key", "order_key", "product_key", "seller_key"],

    "fact_payments": ["payment_key", "order_key"],

    "fact_reviews": ["review_key", "order_key"],

    "fact_customer_order": ["customer_order_key", "customer_key"],

}

def enforce_bigint_columns(df: DataFrame, table_name: str) -> DataFrame:

    """Force all warehouse BIGINT keys to Spark LongType before Parquet write."""

    for column in BIGINT_COLUMNS.get(table_name, []):

        if column in df.columns:

            df = df.withColumn(column, F.col(column).cast(LongType()))

    return df

def validate_bigint_schema(df: DataFrame, table_name: str) -> None:

    """Fail fast if a warehouse BIGINT column is not Spark LongType."""

    field_types = {field.name: field.dataType for field in df.schema.fields}

    for column in BIGINT_COLUMNS.get(table_name, []):

        if column not in field_types:

            continue

        if not isinstance(field_types[column], LongType):

            raise RuntimeError(

                f"{table_name}.{column} must be LongType, "

                f"got {field_types[column]}"

            )

    print(f"[SCHEMA] {table_name}: BIGINT columns are LongType PASS")

def write(df: DataFrame, path: str) -> None:

    """Write curated dataset as Parquet + Snappy with explicit BIGINT preservation."""

    df = enforce_bigint_columns(df, path)

    validate_bigint_schema(df, path)

    print(f"[WRITE] {path}: schema validated before Parquet write")

    (

        df.write

        .mode("overwrite")

        .format("parquet")

        .option("compression", "snappy")

        .save(f"{CURATED}/{path}")

    )

# ============================================================

# QUARANTINE

# ============================================================

def quarantine(

    df: DataFrame,

    path: str,

    reason: str,

) -> None:

    """

    Store invalid records separately.

    RAW is never modified.

    """

    if df.limit(1).count() > 0:

        (

            df.withColumn(

                "_quarantine_reason",

                F.lit(reason),

            )

            .write

            .mode("overwrite")

            .format("parquet")

            .save(f"{QUARANTINE}/{path}")

        )

        print(

            f"[QUARANTINE] {path}: "

            f"{df.count()} rows | reason={reason}"

        )

# ============================================================

# QUALITY CHECK

# ============================================================

def unique(

    df: DataFrame,

    keys: list[str],

    name: str,

) -> None:

    duplicated = (

        df.groupBy(*keys)

        .count()

        .filter(F.col("count") > 1)

        .limit(1)

        .count()

    )

    if duplicated:

        raise RuntimeError(

            f"Uniqueness failed: {name} {keys}"

        )

    print(

        f"[QUALITY] {name}: "

        f"rows={df.count()} "

        f"unique={keys} PASS"

    )

def null_check(

    df: DataFrame,

    column: str,

    name: str,

) -> None:

    n = df.filter(F.col(column).isNull()).limit(1).count()

    if n:

        print(

            f"[QUALITY] {name}: "

            f"{column} contains NULL"

        )

    else:

        print(

            f"[QUALITY] {name}: "

            f"{column} NOT NULL PASS"

        )

# ============================================================

def fk_check(child_df: DataFrame, parent_df: DataFrame, child_key: str, parent_key: str, name: str) -> None:

    """Fail ETL if a non-null child foreign key is absent from the parent."""

    parent_keys = parent_df.select(F.col(parent_key).alias("_parent_key")).dropDuplicates()

    orphan_count = (

        child_df

        .filter(F.col(child_key).isNotNull())

        .join(parent_keys, F.col(child_key) == F.col("_parent_key"), "left_anti")

        .count()

    )

    if orphan_count:

        print(f"[QUALITY] {name}: FAILED - {orphan_count} orphan foreign keys")

        raise RuntimeError(f"Foreign key check failed: {name}; {orphan_count} orphan rows")

    print(f"[QUALITY] {name}: FK PASS")

# FOREIGN KEY CHECK

# ============================================================

def quarantine_orphans(

    fact_df: DataFrame,

    dimension_df: DataFrame,

    fact_key: str,

    dimension_key: str,

    path: str,

    reason: str,

) -> DataFrame:

    """

    Find fact records whose foreign key does not exist

    in the corresponding dimension.

    """

    dim_keys = (

        dimension_df

        .select(F.col(dimension_key).alias("_dim_key"))

        .dropDuplicates()

    )

    bad = (

        fact_df

        .join(

            dim_keys,

            F.col(fact_key) == F.col("_dim_key"),

            "left",

        )

        .filter(

            F.col(fact_key).isNull()

            | F.col("_dim_key").isNull()

        )

        .drop("_dim_key")

    )

    quarantine(bad, path, reason)

    return (

        fact_df

        .join(

            dim_keys,

            F.col(fact_key) == F.col("_dim_key"),

            "inner",

        )

        .drop("_dim_key")

    )

# ============================================================

# GEOLOCATION DIMENSION

# ============================================================

def geo_dim(spark: SparkSession) -> DataFrame:

    print("[TRANSFORM] geolocation")

    d = strings(

        read(

            spark,

            "geolocation",

            GEO,

        )

    ).withColumn(

        "geolocation_zip_code_prefix",

        normalize_zip(F.col("geolocation_zip_code_prefix")),

    )

    bad = d.filter(

        F.col("geolocation_zip_code_prefix").isNull()

        | ~F.col("geolocation_lat").between(-90, 90)

        | ~F.col("geolocation_lng").between(-180, 180)

    )

    quarantine(

        bad,

        "geolocation/invalid",

        "invalid zip/coordinates",

    )

    d = d.filter(

        F.col("geolocation_zip_code_prefix").isNotNull()

        & F.col("geolocation_lat").between(-90, 90)

        & F.col("geolocation_lng").between(-180, 180)

    )

    # RAW geolocation contains many rows per ZIP prefix.

    # Reduce to exactly one row per ZIP prefix.

    d = (

        d.groupBy("geolocation_zip_code_prefix")

        .agg(

            F.avg("geolocation_lat").alias("latitude"),

            F.avg("geolocation_lng").alias("longitude"),

            F.first(

                "geolocation_city",

                ignorenulls=True,

            ).alias("city"),

            F.first(

                "geolocation_state",

                ignorenulls=True,

            ).alias("state"),

            F.count("*").alias("source_row_count"),

        )

        .withColumnRenamed(

            "geolocation_zip_code_prefix",

            "zip_code_prefix",

        )

        .withColumn(

            "geolocation_key",

            F.xxhash64("zip_code_prefix").cast(LongType()),

        )

    )

    d = d.select(

        "geolocation_key",

        "zip_code_prefix",

        "city",

        "state",

        "latitude",

        "longitude",

        "source_row_count",

    )

    write(d, "dim_geolocation")

    return d

# ============================================================

# CATEGORY DIMENSION

# ============================================================

def category_dim(spark: SparkSession) -> DataFrame:

    print("[TRANSFORM] category")

    d = strings(

        read(

            spark,

            "categories",

            CATEGORIES,

        )

    )

    d = (

        d.filter(

            F.col("product_category_name").isNotNull()

        )

        .dropDuplicates(["product_category_name"])

        .withColumn(

            "category_key",

            F.xxhash64(

                "product_category_name"

            ).cast(LongType()),

        )

        .select(

            "category_key",

            "product_category_name",

            "product_category_name_english",

        )

    )

    write(d, "dim_category")

    return d

# ============================================================

# CUSTOMER DIMENSION

# ============================================================

def customer_dim(

    spark: SparkSession,

    geo: DataFrame,

) -> tuple[DataFrame, DataFrame]:

    print("[TRANSFORM] customer")

    d = strings(

        read(

            spark,

            "customers",

            CUSTOMERS,

        )

    ).withColumn(

        "customer_zip_code_prefix",

        normalize_zip(F.col("customer_zip_code_prefix")),

    )

    bad = d.filter(

        F.col("customer_id").isNull()

        | F.col("customer_unique_id").isNull()

    )

    quarantine(

        bad,

        "customers/invalid",

        "missing customer keys",

    )

    d = d.filter(

        F.col("customer_id").isNotNull()

        & F.col("customer_unique_id").isNotNull()

    )

    geo_lookup = (

        geo

        .select(

            "geolocation_key",

            F.col("zip_code_prefix").alias("zip"),

        )

        .alias("g")

    )

    # --------------------------------------------------------

    # Dimension

    # Grain: 1 row / customer_unique_id

    # --------------------------------------------------------

    dim = (

        d.dropDuplicates(

            ["customer_unique_id"]

        )

        .alias("c")

        .join(

            geo_lookup,

            F.col(

                "c.customer_zip_code_prefix"

            ) == F.col("g.zip"),

            "left",

        )

        .withColumn(

            "customer_key",

            F.xxhash64(

                "c.customer_unique_id"

            ).cast(LongType()),

        )

        .select(

            "customer_key",

            F.col(

                "c.customer_unique_id"

            ).alias("customer_unique_id"),

            F.col(

                "c.customer_city"

            ).alias("city"),

            F.col(

                "c.customer_state"

            ).alias("state"),

            F.col(

                "g.geolocation_key"

            ).cast(LongType()).alias("geolocation_key"),

        )

    )

    print("[SCHEMA CHECK] dim_customer")

    dim.printSchema()

    if not isinstance(dim.schema["geolocation_key"].dataType, LongType):

        raise RuntimeError(

            "dim_customer.geolocation_key must be LongType before write"

        )

    print("[SCHEMA CHECK] dim_customer.geolocation_key: LongType PASS")

    # --------------------------------------------------------

    # Mapping

    #

    # customer_id → customer_unique_id → customer_key

    #

    # This is required because orders use customer_id,

    # while the customer dimension uses customer_unique_id.

    # --------------------------------------------------------

    mapping = (

        d.select(

            "customer_id",

            "customer_unique_id",

        )

        .withColumn(

            "customer_key",

            F.xxhash64(

                "customer_unique_id"

            ).cast(LongType()),

        )

        .select(

            "customer_id",

            "customer_key",

        )

        .dropDuplicates(["customer_id"])

    )

    write(dim, "dim_customer")

    return dim, mapping

# ============================================================

# SELLER DIMENSION

# ============================================================

def seller_dim(

    spark: SparkSession,

    geo: DataFrame,

) -> DataFrame:

    print("[TRANSFORM] seller")

    d = strings(

        read(

            spark,

            "sellers",

            SELLERS,

        )

    ).withColumn(

        "seller_zip_code_prefix",

        normalize_zip(F.col("seller_zip_code_prefix")),

    )

    bad = d.filter(

        F.col("seller_id").isNull()

    )

    quarantine(

        bad,

        "sellers/invalid",

        "missing seller_id",

    )

    d = (

        d.filter(

            F.col("seller_id").isNotNull()

        )

        .dropDuplicates(["seller_id"])

        .alias("x")

    )

    geo_lookup = (

        geo

        .select(

            "geolocation_key",

            F.col("zip_code_prefix").alias("zip"),

        )

        .alias("g")

    )

    d = (

        d.join(

            geo_lookup,

            F.col(

                "x.seller_zip_code_prefix"

            ) == F.col("g.zip"),

            "left",

        )

        .withColumn(

            "seller_key",

            F.xxhash64(

                "x.seller_id"

            ).cast(LongType()),

        )

        .select(

            "seller_key",

            F.col("x.seller_id").alias("seller_id"),

            F.col("x.seller_city").alias("city"),

            F.col("x.seller_state").alias("state"),

            F.col("g.geolocation_key").cast(LongType()).alias(

                "geolocation_key"

            ),

        )

    )

    print("[SCHEMA CHECK] dim_seller")

    d.printSchema()

    if not isinstance(d.schema["geolocation_key"].dataType, LongType):

        raise RuntimeError(

            "dim_seller.geolocation_key must be LongType before write"

        )

    print("[SCHEMA CHECK] dim_seller.geolocation_key: LongType PASS")

    write(d, "dim_seller")

    return d

# ============================================================

# PRODUCT DIMENSION

# ============================================================

def product_dim(

    spark: SparkSession,

    categories: DataFrame,

) -> DataFrame:

    print("[TRANSFORM] product")

    d = strings(

        read(

            spark,

            "products",

            PRODUCTS,

        )

    )

    bad = d.filter(

        F.col("product_id").isNull()

    )

    quarantine(

        bad,

        "products/invalid",

        "missing product_id",

    )

    d = (

        d.filter(

            F.col("product_id").isNotNull()

        )

        .dropDuplicates(["product_id"])

        .alias("p")

    )

    category_lookup = (

        categories

        .select(

            "category_key",

            "product_category_name",

        )

        .alias("c")

    )

    d = (

        d.join(

            category_lookup,

            F.col(

                "p.product_category_name"

            )

            == F.col(

                "c.product_category_name"

            ),

            "left",

        )

        .withColumn(

            "product_key",

            F.xxhash64(

                "p.product_id"

            ).cast(LongType()),

        )

        .select(

            "product_key",

            F.col("p.product_id").alias(

                "product_id"

            ),

            F.col("c.category_key").cast(LongType()).alias(

                "category_key"

            ),

            F.col(

                "p.product_category_name"

            ).alias(

                "product_category_name"

            ),

            F.col(

                "p.product_name_lenght"

            ).alias(

                "product_name_length"

            ),

            F.col(

                "p.product_description_lenght"

            ).alias(

                "product_description_length"

            ),

            "product_photos_qty",

            "product_weight_g",

            "product_length_cm",

            "product_height_cm",

            "product_width_cm",

        )

    )

    write(d, "dim_product")

    return d

# ============================================================

# DATE DIMENSION

# ============================================================

def date_dim(

    spark: SparkSession,

    orders: DataFrame,

) -> DataFrame:

    print("[TRANSFORM] date")

    dates = (

        orders

        .select(

            F.to_date(

                "order_purchase_timestamp"

            ).alias("date")

        )

        .filter(F.col("date").isNotNull())

        .distinct()

    )

    bounds = (

        dates

        .agg(

            F.min("date").alias("min_date"),

            F.max("date").alias("max_date"),

        )

        .first()

    )

    if bounds["min_date"] is None:

        raise RuntimeError(

            "Cannot build dim_date: "

            "no valid order purchase dates."

        )

    d = (

        spark.range(1)

        .select(

            F.explode(

                F.sequence(

                    F.lit(bounds["min_date"]),

                    F.lit(bounds["max_date"]),

                    F.expr("interval 1 day"),

                )

            ).alias("date")

        )

    )

    d = (

        d.withColumn(

            "date_key",

            F.date_format(

                "date",

                "yyyyMMdd",

            ).cast(IntegerType()),

        )

        .withColumn("year", F.year("date"))

        .withColumn("quarter", F.quarter("date"))

        .withColumn("month", F.month("date"))

        .withColumn(

            "month_name",

            F.date_format("date", "MMMM"),

        )

        .withColumn(

            "week_of_year",

            F.weekofyear("date"),

        )

        .withColumn(

            "day",

            F.dayofmonth("date"),

        )

        .withColumn(

            "day_of_week",

            F.dayofweek("date"),

        )

        .withColumn(

            "is_weekend",

            F.dayofweek("date").isin(1, 7),

        )

    )

    write(d, "dim_date")

    return d

# ============================================================

# ORDERS FACT

# ============================================================

def orders_fact(

    spark: SparkSession,

    customer_mapping: DataFrame,

    date_dim_df: DataFrame,

) -> DataFrame:

    print("[TRANSFORM] orders")

    d = strings(read(spark, "orders", ORDERS))

    bad = d.filter(

        F.col("order_id").isNull()

        | F.col("customer_id").isNull()

        | F.col("order_purchase_timestamp").isNull()

    )

    quarantine(bad, "orders/invalid", "missing required fields")

    d = d.filter(

        F.col("order_id").isNotNull()

        & F.col("customer_id").isNotNull()

        & F.col("order_purchase_timestamp").isNotNull()

    )

    d = d.dropDuplicates(["order_id"]).alias("o")

    allowed_status = [

        "delivered", "shipped", "canceled", "invoiced",

        "processing", "approved", "created", "unavailable",

    ]

    bad_status = d.filter(

        F.col("o.order_status").isNotNull()

        & ~F.col("o.order_status").isin(allowed_status)

    )

    quarantine(bad_status, "orders/invalid_status", "invalid order_status")

    d = d.filter(

        F.col("o.order_status").isNull()

        | F.col("o.order_status").isin(allowed_status)

    )

    customer_lookup = (

        customer_mapping

        .select("customer_id", "customer_key")

        .dropDuplicates(["customer_id"])

        .alias("c")

    )

    d = d.withColumn(

        "purchase_date",

        F.to_date(F.col("o.order_purchase_timestamp")),

    )

    date_lookup = (

        date_dim_df

        .select(

            F.col("date_key").alias("date_key"),

            F.col("date").alias("dim_date_value"),

        )

        .dropDuplicates(["dim_date_value"])

        .alias("dd")

    )

    j = (

        d

        .join(

            customer_lookup,

            F.col("o.customer_id") == F.col("c.customer_id"),

            "left",

        )

        .join(

            date_lookup,

            F.col("purchase_date") == F.col("dd.dim_date_value"),

            "left",

        )

    )

    bad_customer = j.filter(F.col("c.customer_key").isNull())

    quarantine(

        bad_customer,

        "orders/orphan_customer",

        "customer_id not found in customer mapping",

    )

    j = j.filter(F.col("c.customer_key").isNotNull())

    bad_date = j.filter(F.col("dd.date_key").isNull())

    quarantine(

        bad_date,

        "orders/orphan_date",

        "purchase date not found in dim_date",

    )

    j = j.filter(F.col("dd.date_key").isNotNull())

    f = (

        j

        .withColumn(

            "delivery_days",

            F.when(

                F.col("o.order_delivered_customer_date").isNotNull(),

                F.datediff(

                    F.to_date(F.col("o.order_delivered_customer_date")),

                    F.to_date(F.col("o.order_purchase_timestamp")),

                ),

            ),

        )

        .withColumn(

            "estimated_delivery_days",

            F.when(

                F.col("o.order_estimated_delivery_date").isNotNull(),

                F.datediff(

                    F.to_date(F.col("o.order_estimated_delivery_date")),

                    F.to_date(F.col("o.order_purchase_timestamp")),

                ),

            ),

        )

        .withColumn(

            "delivery_delay_days",

            F.when(

                F.col("o.order_delivered_customer_date").isNotNull()

                & F.col("o.order_estimated_delivery_date").isNotNull(),

                F.datediff(

                    F.to_date(F.col("o.order_delivered_customer_date")),

                    F.to_date(F.col("o.order_estimated_delivery_date")),

                ),

            ),

        )

        .withColumn(

            "is_delivered",

            (F.col("o.order_status") == "delivered").cast(IntegerType()),

        )

        .withColumn(

            "is_canceled",

            (F.col("o.order_status") == "canceled").cast(IntegerType()),

        )

        .withColumn(

            "order_key",

            F.xxhash64(F.col("o.order_id")).cast(LongType()),

        )

        .select(

            "order_key",

            F.col("o.order_id").alias("order_id"),

            F.col("c.customer_key").alias("customer_key"),

            F.col("dd.date_key").alias("purchase_date_key"),

            F.col("o.order_status").alias("order_status"),

            F.col("o.order_purchase_timestamp").alias("order_purchase_timestamp"),

            F.col("o.order_approved_at").alias("order_approved_at"),

            F.col("o.order_delivered_carrier_date").alias("order_delivered_carrier_date"),

            F.col("o.order_delivered_customer_date").alias("order_delivered_customer_date"),

            F.col("o.order_estimated_delivery_date").alias("order_estimated_delivery_date"),

            "delivery_days",

            "estimated_delivery_days",

            "delivery_delay_days",

            "is_delivered",

            "is_canceled",

        )

    )

    write(f, "fact_orders")

    return f

def items_fact(

    spark: SparkSession,

    orders: DataFrame,

    products: DataFrame,

    sellers: DataFrame,

) -> DataFrame:

    print("[TRANSFORM] order_items")

    d = strings(

        read(

            spark,

            "order_items",

            ITEMS,

        )

    )

    # --------------------------------------------------------

    # Required fields

    # --------------------------------------------------------

    bad = d.filter(

        F.col("order_id").isNull()

        | F.col("order_item_id").isNull()

        | F.col("product_id").isNull()

        | F.col("seller_id").isNull()

    )

    quarantine(

        bad,

        "order_items/invalid",

        "missing required keys",

    )

    d = d.filter(

        F.col("order_id").isNotNull()

        & F.col("order_item_id").isNotNull()

        & F.col("product_id").isNotNull()

        & F.col("seller_id").isNotNull()

    )

    # --------------------------------------------------------

    # Domain validation

    # --------------------------------------------------------

    bad_price = d.filter(

        F.col("price").isNotNull()

        & (F.col("price") < 0)

    )

    quarantine(

        bad_price,

        "order_items/invalid_price",

        "price < 0",

    )

    bad_freight = d.filter(

        F.col("freight_value").isNotNull()

        & (F.col("freight_value") < 0)

    )

    quarantine(

        bad_freight,

        "order_items/invalid_freight",

        "freight_value < 0",

    )

    d = d.filter(

        F.col("price").isNull()

        | (F.col("price") >= 0)

    ).filter(

        F.col("freight_value").isNull()

        | (F.col("freight_value") >= 0)

    )

    # One row per order + item

    d = (

        d.dropDuplicates(

            ["order_id", "order_item_id"]

        )

        .alias("i")

    )

    # --------------------------------------------------------

    # Join orders

    # --------------------------------------------------------

    order_lookup = (

        orders

        .select(

            "order_id",

            "order_key",

            "order_purchase_timestamp",

        )

        .alias("o")

    )

    product_lookup = (

        products

        .select(

            "product_key",

            "product_id",

        )

        .alias("p")

    )

    seller_lookup = (

        sellers

        .select(

            "seller_key",

            "seller_id",

        )

        .alias("s")

    )

    j = (

        d.join(

            order_lookup,

            F.col("i.order_id")

            == F.col("o.order_id"),

            "left",

        )

        .join(

            product_lookup,

            F.col("i.product_id")

            == F.col("p.product_id"),

            "left",

        )

        .join(

            seller_lookup,

            F.col("i.seller_id")

            == F.col("s.seller_id"),

            "left",

        )

    )

    # --------------------------------------------------------

    # Orphan FK records

    # --------------------------------------------------------

    bad_order = j.filter(

        F.col("o.order_key").isNull()

    )

    quarantine(

        bad_order,

        "order_items/orphan_order",

        "order_id not found in fact_orders",

    )

    bad_product = j.filter(

        F.col("p.product_key").isNull()

    )

    quarantine(

        bad_product,

        "order_items/orphan_product",

        "product_id not found in dim_product",

    )

    bad_seller = j.filter(

        F.col("s.seller_key").isNull()

    )

    quarantine(

        bad_seller,

        "order_items/orphan_seller",

        "seller_id not found in dim_seller",

    )

    # Keep only records with valid relationships

    j = j.filter(

        F.col("o.order_key").isNotNull()

        & F.col("p.product_key").isNotNull()

        & F.col("s.seller_key").isNotNull()

    )

    # --------------------------------------------------------

    # Derived fields

    # --------------------------------------------------------

    f = (

        j

        .withColumn(

            "date_key",

            F.date_format(

                F.to_date(

                    "o.order_purchase_timestamp"

                ),

                "yyyyMMdd",

            ).cast(IntegerType()),

        )

        .withColumn(

            "item_key",

            F.xxhash64(

                "i.order_id",

                "i.order_item_id",

            ).cast(LongType()),

        )

        .withColumn(

            "item_total",

            F.coalesce(

                F.col("i.price"),

                F.lit(0.0),

            )

            + F.coalesce(

                F.col("i.freight_value"),

                F.lit(0.0),

            ),

        )

        .select(

            "item_key",

            F.col("o.order_key").alias(

                "order_key"

            ),

            F.col("i.order_id").alias(

                "order_id"

            ),

            F.col("i.order_item_id").alias(

                "order_item_id"

            ),

            F.col("p.product_key").alias(

                "product_key"

            ),

            F.col("s.seller_key").alias(

                "seller_key"

            ),

            "date_key",

            F.col(

                "i.shipping_limit_date"

            ).alias(

                "shipping_limit_date"

            ),

            F.col("i.price").alias("price"),

            F.col(

                "i.freight_value"

            ).alias(

                "freight_value"

            ),

            "item_total",

        )

    )

    write(f, "fact_order_items")

    return f

# ============================================================

# PAYMENTS FACT

# ============================================================

def payments_fact(

    spark: SparkSession,

    orders: DataFrame,

) -> DataFrame:

    print("[TRANSFORM] payments")

    d = strings(

        read(

            spark,

            "payments",

            PAYMENTS,

        )

    )

    bad = d.filter(

        F.col("order_id").isNull()

        | F.col("payment_sequential").isNull()

    )

    quarantine(

        bad,

        "payments/invalid",

        "missing required keys",

    )

    d = d.filter(

        F.col("order_id").isNotNull()

        & F.col("payment_sequential").isNotNull()

    )

    bad_value = d.filter(

        F.col("payment_value").isNotNull()

        & (F.col("payment_value") < 0)

    )

    quarantine(

        bad_value,

        "payments/invalid_value",

        "payment_value < 0",

    )

    d = d.filter(

        F.col("payment_value").isNull()

        | (F.col("payment_value") >= 0)

    )

    d = (

        d.dropDuplicates(

            ["order_id", "payment_sequential"]

        )

        .alias("p")

    )

    order_lookup = (

        orders

        .select(

            "order_id",

            "order_key",

        )

        .alias("o")

    )

    j = d.join(

        order_lookup,

        F.col("p.order_id")

        == F.col("o.order_id"),

        "left",

    )

    bad_order = j.filter(

        F.col("o.order_key").isNull()

    )

    quarantine(

        bad_order,

        "payments/orphan_order",

        "order_id not found in fact_orders",

    )

    j = j.filter(

        F.col("o.order_key").isNotNull()

    )

    f = (

        j

        .withColumn(

            "payment_key",

            F.xxhash64(

                "p.order_id",

                "p.payment_sequential",

            ).cast(LongType()),

        )

        .select(

            "payment_key",

            F.col("o.order_key").alias(

                "order_key"

            ),

            F.col("p.order_id").alias(

                "order_id"

            ),

            F.col(

                "p.payment_sequential"

            ).alias(

                "payment_sequential"

            ),

            F.col("p.payment_type").alias(

                "payment_type"

            ),

            F.col(

                "p.payment_installments"

            ).alias(

                "payment_installments"

            ),

            F.col("p.payment_value").alias(

                "payment_value"

            ),

        )

    )

    write(f, "fact_payments")

    return f

# ============================================================

# REVIEWS FACT

# ============================================================

def reviews_fact(

    spark: SparkSession,

    orders: DataFrame,

) -> DataFrame:

    print("[TRANSFORM] reviews")

    d = strings(

        read(

            spark,

            "reviews",

            REVIEWS,

        )

    )

    bad = d.filter(

        F.col("order_id").isNull()

        | F.col("review_id").isNull()

        | F.col("review_score").isNull()

    )

    quarantine(

        bad,

        "reviews/invalid",

        "missing required keys",

    )

    d = d.filter(

        F.col("order_id").isNotNull()

        & F.col("review_id").isNotNull()

        & F.col("review_score").between(1, 5)

    )

    bad_score = d.filter(

        ~F.col("review_score").between(1, 5)

    )

    quarantine(

        bad_score,

        "reviews/invalid_score",

        "review_score must be between 1 and 5",

    )

    d = (

        d.filter(

            F.col("review_score").between(1, 5)

        )

        .dropDuplicates(["review_id"])

        .alias("r")

    )

    order_lookup = (

        orders

        .select(

            "order_id",

            "order_key",

        )

        .alias("o")

    )

    j = d.join(

        order_lookup,

        F.col("r.order_id")

        == F.col("o.order_id"),

        "left",

    )

    bad_order = j.filter(

        F.col("o.order_key").isNull()

    )

    quarantine(

        bad_order,

        "reviews/orphan_order",

        "order_id not found in fact_orders",

    )

    j = j.filter(

        F.col("o.order_key").isNotNull()

    )

    f = (

        j

        .withColumn(

            "review_key",

            F.xxhash64(

                "r.review_id"

            ).cast(LongType()),

        )

        .withColumn(

            "is_positive",

            (

                F.col("r.review_score") >= 4

            ).cast(IntegerType()),

        )

        .withColumn(

            "is_negative",

            (

                F.col("r.review_score") <= 2

            ).cast(IntegerType()),

        )

        .select(

            "review_key",

            F.col("o.order_key").alias(

                "order_key"

            ),

            F.col("r.order_id").alias(

                "order_id"

            ),

            F.col("r.review_id").alias(

                "review_id"

            ),

            F.col(

                "r.review_score"

            ).alias(

                "review_score"

            ),

            F.col(

                "r.review_comment_title"

            ).alias(

                "review_comment_title"

            ),

            F.col(

                "r.review_comment_message"

            ).alias(

                "review_comment_message"

            ),

            F.col(

                "r.review_creation_date"

            ).alias(

                "review_creation_date"

            ),

            F.col(

                "r.review_answer_timestamp"

            ).alias(

                "review_answer_timestamp"

            ),

            "is_positive",

            "is_negative",

        )

    )

    write(f, "fact_reviews")

    return f

# ============================================================

# CUSTOMER AGGREGATION

# ============================================================

def customer_aggregate(

    orders: DataFrame,

    items: DataFrame,

    payments: DataFrame,

) -> DataFrame:

    print("[TRANSFORM] customer aggregate")

    # --------------------------------------------------------

    # Aggregate items to order grain FIRST.

    # Avoid order × item × payment fan-out.

    # --------------------------------------------------------

    item_by_order = (

        items

        .groupBy("order_key")

        .agg(

            F.sum(

                "item_total"

            ).alias(

                "order_item_total"

            )

        )

    )

    # --------------------------------------------------------

    # Aggregate payments to order grain FIRST.

    # --------------------------------------------------------

    payment_by_order = (

        payments

        .groupBy("order_key")

        .agg(

            F.sum(

                "payment_value"

            ).alias(

                "order_payment_total"

            )

        )

    )

    # --------------------------------------------------------

    # One row per order before customer aggregation

    # --------------------------------------------------------

    per_order = (

        orders

        .select(

            "order_key",

            "customer_key",

            "order_id",

            "order_purchase_timestamp",

            "is_delivered",

            "is_canceled",

            "delivery_days",

        )

        .join(

            item_by_order,

            "order_key",

            "left",

        )

        .join(

            payment_by_order,

            "order_key",

            "left",

        )

    )

    # --------------------------------------------------------

    # Customer-level aggregation

    # Grain: 1 row / customer

    # --------------------------------------------------------

    f = (

        per_order

        .groupBy("customer_key")

        .agg(

            F.countDistinct(

                "order_id"

            ).alias(

                "order_count"

            ),

            F.sum(

                "is_delivered"

            ).alias(

                "delivered_order_count"

            ),

            F.sum(

                "is_canceled"

            ).alias(

                "canceled_order_count"

            ),

            F.sum(

                F.coalesce(

                    F.col(

                        "order_payment_total"

                    ),

                    F.lit(0.0),

                )

            ).alias(

                "total_spent"

            ),

            F.avg(

                "order_payment_total"

            ).alias(

                "average_order_value"

            ),

            F.min(

                "order_purchase_timestamp"

            ).alias(

                "first_order_timestamp"

            ),

            F.max(

                "order_purchase_timestamp"

            ).alias(

                "last_order_timestamp"

            ),

            F.avg(

                "delivery_days"

            ).alias(

                "average_delivery_days"

            ),

        )

        .withColumn(

            "customer_order_key",

            F.xxhash64(

                "customer_key"

            ).cast(LongType()),

        )

        .select(

            "customer_order_key",

            "customer_key",

            "order_count",

            "delivered_order_count",

            "canceled_order_count",

            "total_spent",

            "average_order_value",

            "first_order_timestamp",

            "last_order_timestamp",

            "average_delivery_days",

        )

    )

    write(

        f,

        "fact_customer_order",

    )

    return f

# ============================================================

# MAIN

# ============================================================

def main():

    print("[START] Olist Spark ETL")

    print("[KEY POLICY] All xxhash64 warehouse keys are preserved as Spark LongType/Parquet INT64")

    print(

        f"[CONFIG] RAW={RAW}"

    )

    print(

        f"[CONFIG] CURATED={CURATED}"

    )

    print(

        f"[CONFIG] QUARANTINE={QUARANTINE}"

    )

    spark = spark_session()

    spark.sparkContext.setLogLevel("WARN")

    try:

        # ====================================================

        # RAW ORDERS

        # ====================================================

        print(

            "[STEP] reading raw orders"

        )

        raw_orders = strings(

            read(

                spark,

                "orders",

                ORDERS,

            )

        )

        # ====================================================

        # DIMENSIONS

        # ====================================================

        print(

            "[STEP] dimensions: geolocation"

        )

        geo = geo_dim(spark)

        print(

            "[STEP] dimensions: category"

        )

        categ = category_dim(spark)

        print(

            "[STEP] dimensions: customer"

        )

        customer, customer_mapping = (

            customer_dim(

                spark,

                geo,

            )

        )

        print(

            "[STEP] dimensions: seller"

        )

        seller = seller_dim(

            spark,

            geo,

        )

        print(

            "[STEP] dimensions: product"

        )

        product = product_dim(

            spark,

            categ,

        )

        print(

            "[STEP] dimensions: date"

        )

        date = date_dim(

            spark,

            raw_orders,

        )

        # ====================================================

        # FACT: ORDERS

        # ====================================================

        print(

            "[STEP] fact: orders"

        )

        orders = orders_fact(

            spark,

            customer_mapping,

            date,

        )

        # ====================================================

        # FACT: ORDER ITEMS

        # ====================================================

        print(

            "[STEP] fact: order_items"

        )

        items = items_fact(

            spark,

            orders,

            product,

            seller,

        )

        # ====================================================

        # FACT: PAYMENTS

        # ====================================================

        print(

            "[STEP] fact: payments"

        )

        payments = payments_fact(

            spark,

            orders,

        )

        # ====================================================

        # FACT: REVIEWS

        # ====================================================

        print(

            "[STEP] fact: reviews"

        )

        reviews = reviews_fact(

            spark,

            orders,

        )

        # ====================================================

        # AGGREGATE

        # ====================================================

        print(

            "[STEP] aggregate: customer_order"

        )

        customer_order = customer_aggregate(

            orders,

            items,

            payments,

        )

        # ====================================================

        # QUALITY CHECKS

        # ====================================================

        print(

            "[STEP] quality checks"

        )

        unique(

            geo,

            ["geolocation_key"],

            "dim_geolocation",

        )

        unique(

            categ,

            ["category_key"],

            "dim_category",

        )

        unique(

            customer,

            ["customer_key"],

            "dim_customer",

        )

        unique(

            customer_mapping,

            ["customer_id"],

            "customer_mapping",

        )

        unique(

            seller,

            ["seller_key"],

            "dim_seller",

        )

        fk_check(

            customer,

            geo,

            "geolocation_key",

            "geolocation_key",

            "dim_customer.geolocation_key -> dim_geolocation.geolocation_key",

        )

        fk_check(

            seller,

            geo,

            "geolocation_key",

            "geolocation_key",

            "dim_seller.geolocation_key -> dim_geolocation.geolocation_key",

        )

        fk_check(

            product,

            categ,

            "category_key",

            "category_key",

            "dim_product.category_key -> dim_category.category_key",

        )

        unique(

            product,

            ["product_key"],

            "dim_product",

        )

        unique(

            date,

            ["date_key"],

            "dim_date",

        )

        unique(

            orders,

            ["order_key"],

            "fact_orders",

        )

        unique(

            orders,

            ["order_id"],

            "fact_orders.order_id",

        )

        unique(

            items,

            ["order_id", "order_item_id"],

            "fact_order_items",

        )

        unique(

            payments,

            ["order_id", "payment_sequential"],

            "fact_payments",

        )

        unique(

            reviews,

            ["review_key"],

            "fact_reviews",

        )

        unique(

            customer_order,

            ["customer_order_key"],

            "fact_customer_order",

        )

        # ----------------------------------------------------

        # Foreign key quality checks

        # ----------------------------------------------------

        fk_check(

            orders,

            customer,

            "customer_key",

            "customer_key",

            "fact_orders.customer_key -> dim_customer.customer_key",

        )

        fk_check(

            orders,

            date,

            "purchase_date_key",

            "date_key",

            "fact_orders.purchase_date_key -> dim_date.date_key",

        )

        fk_check(

            items,

            orders,

            "order_key",

            "order_key",

            "fact_order_items.order_key -> fact_orders.order_key",

        )

        fk_check(

            items,

            product,

            "product_key",

            "product_key",

            "fact_order_items.product_key -> dim_product.product_key",

        )

        fk_check(

            items,

            seller,

            "seller_key",

            "seller_key",

            "fact_order_items.seller_key -> dim_seller.seller_key",

        )

        fk_check(

            items,

            date,

            "date_key",

            "date_key",

            "fact_order_items.date_key -> dim_date.date_key",

        )

        fk_check(

            payments,

            orders,

            "order_key",

            "order_key",

            "fact_payments.order_key -> fact_orders.order_key",

        )

        fk_check(

            reviews,

            orders,

            "order_key",

            "order_key",

            "fact_reviews.order_key -> fact_orders.order_key",

        )

        fk_check(

            customer_order,

            customer,

            "customer_key",

            "customer_key",

            "fact_customer_order.customer_key -> dim_customer.customer_key",

        )

        null_check(

            orders,

            "customer_key",

            "fact_orders",

        )

        null_check(

            orders,

            "purchase_date_key",

            "fact_orders",

        )

        null_check(

            items,

            "order_key",

            "fact_order_items",

        )

        null_check(

            items,

            "product_key",

            "fact_order_items",

        )

        null_check(

            items,

            "seller_key",

            "fact_order_items",

        )

        null_check(

            payments,

            "order_key",

            "fact_payments",

        )

        null_check(

            reviews,

            "order_key",

            "fact_reviews",

        )

        print(

            "[FINISHED] "

            "Olist Spark ETL completed successfully"

        )

    finally:

        spark.stop()

if __name__ == "__main__":

    main()
