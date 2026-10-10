import io

import os

import time

from decimal import Decimal

from datetime import datetime, date

import boto3

import pandas as pd

from botocore.client import Config

from sqlalchemy import create_engine, text

# ============================================================

# CONFIG

# ============================================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "host.docker.internal:9000")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")

MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "olist")

MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql-dw")

MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))

MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "olist_dw")

MYSQL_USER = os.getenv("MYSQL_USER", "olist")

MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "olist123")

CURATED_PREFIX = os.getenv("CURATED_PREFIX", "curated").strip("/")

LOAD_MODE = os.getenv("LOAD_MODE", "replace").lower()

# Load order MUST respect FK dependencies.

TABLES = [

    ("dim_geolocation", "dim_geolocation"),

    ("dim_category", "dim_category"),

    ("dim_customer", "dim_customer"),

    ("dim_seller", "dim_seller"),

    ("dim_product", "dim_product"),

    ("dim_date", "dim_date"),

    ("fact_orders", "fact_orders"),

    ("fact_order_items", "fact_order_items"),

    ("fact_payments", "fact_payments"),

    ("fact_reviews", "fact_reviews"),

    ("fact_customer_order", "fact_customer_order"),

]

# Spark LongType -> MySQL BIGINT.

# These columns MUST NEVER be allowed to pass through float64.

INT_COLUMNS = {

    "geolocation_key",

    "category_key",

    "customer_key",

    "seller_key",

    "product_key",

    "date_key",

    "order_key",

    "item_key",

    "payment_key",

    "review_key",

    "customer_order_key",

}

# ============================================================

# MINIO

# ============================================================

def minio_client():

    protocol = "https" if MINIO_SECURE else "http"

    endpoint = f"{protocol}://{MINIO_ENDPOINT}"

    return boto3.client(

        "s3",

        endpoint_url=endpoint,

        aws_access_key_id=MINIO_ACCESS_KEY,

        aws_secret_access_key=MINIO_SECRET_KEY,

        config=Config(signature_version="s3v4"),

        region_name="us-east-1",

    )

def list_parquet_keys(s3, prefix):

    paginator = s3.get_paginator("list_objects_v2")

    keys = []

    for page in paginator.paginate(

        Bucket=MINIO_BUCKET,

        Prefix=prefix,

    ):

        for obj in page.get("Contents", []):

            key = obj["Key"]

            if key.endswith(".parquet"):

                keys.append(key)

    return sorted(keys)

def read_curated_table(s3, path):

    prefix = f"{CURATED_PREFIX}/{path}/"

    keys = list_parquet_keys(s3, prefix)

    if not keys:

        raise RuntimeError(

            f"No parquet files found: "

            f"s3://{MINIO_BUCKET}/{prefix}"

        )

    frames = []

    for key in keys:

        print(

            f"[MINIO] reading "

            f"s3://{MINIO_BUCKET}/{key}"

        )

        obj = s3.get_object(

            Bucket=MINIO_BUCKET,

            Key=key,

        )

        data = obj["Body"].read()

        frames.append(

            pd.read_parquet(

                io.BytesIO(data),

                engine="pyarrow",

                # Preserve nullable INT64/BIGINT as Arrow integers.

                # NumPy-backed reads can turn nullable int64 into float64,

                # corrupting large hash keys beyond 2**53.

                dtype_backend="pyarrow",

            )

        )

    df = pd.concat(

        frames,

        ignore_index=True,

    )

    print(

        f"[CURATED] {path}: "

        f"rows={len(df):,}, "

        f"columns={len(df.columns)}"

    )

    return df

# ============================================================

# MYSQL

# ============================================================

def mysql_engine():

    url = (

        f"mysql+pymysql://"

        f"{MYSQL_USER}:{MYSQL_PASSWORD}"

        f"@{MYSQL_HOST}:{MYSQL_PORT}"

        f"/{MYSQL_DATABASE}"

        f"?charset=utf8mb4"

    )

    return create_engine(

        url,

        pool_pre_ping=True,

        future=True,

    )

def wait_mysql(engine):

    for i in range(60):

        try:

            with engine.connect() as conn:

                conn.execute(text("SELECT 1"))

            print("[MYSQL] connected")

            return

        except Exception:

            if i == 59:

                raise

            time.sleep(2)

# ============================================================

# TYPE NORMALIZATION

# ============================================================

def normalize_dataframe_types(table, df):

    """

    Normalize values before sending them to MySQL.

    Critical rule:

        Spark LongType -> pandas Int64 -> Python int

        NEVER:

        Spark LongType -> pandas float64 -> Python float

    """

    df = df.copy()

    # --------------------------------------------------------

    # 1. Preserve all Spark LongType keys as exact Int64.

    # --------------------------------------------------------

    for col in df.columns:

        if col not in INT_COLUMNS:

            continue

        try:

            series = df[col]

            # Do not cast a float-backed BIGINT back to integer: values may

            # already have lost precision when they were converted to float64.

            # dtype_backend="pyarrow" should preserve Parquet INT64 columns

            # as exact integer arrays, including columns containing NULLs.

            if pd.api.types.is_float_dtype(series.dtype):

                raise TypeError(

                    f"{table}.{col} was read as floating point "

                    f"({series.dtype}); refusing a potentially lossy "

                    "BIGINT conversion. Check the Parquet schema and ensure "

                    "read_parquet uses dtype_backend='pyarrow'."

                )

            # Direct integer-to-Int64 cast; never route BIGINT through float.

            df[col] = series.astype("Int64")

        except Exception as exc:

            raise ValueError(

                f"Cannot safely convert column '{col}' in table '{table}' "

                "to pandas Int64 without precision loss"

            ) from exc

    # --------------------------------------------------------

    # 2. Normalize datetime/date columns.

    # --------------------------------------------------------

    for col in df.columns:

        is_datetime_column = (

            "timestamp" in col

            or col.endswith("_date")

            or col == "date"

        )

        if not is_datetime_column:

            continue

        # Already datetime64 -> leave unchanged.

        if pd.api.types.is_datetime64_any_dtype(df[col]):

            continue

        try:

            parsed = pd.to_datetime(

                df[col],

                errors="raise",

            )

            df[col] = parsed

        except (TypeError, ValueError):

            # Some \\\\*_date fields can be strings that should remain

            # strings if they are not actually datetime values.

            pass

    return df

def normalize_value(v):

    """

    Convert pandas/numpy values to native Python values

    that PyMySQL can safely bind.

    """

    # pandas NA / NaN / NaT

    if v is None:

        return None

    try:

        missing = pd.isna(v)

        if isinstance(missing, bool) and missing:

            return None

    except (TypeError, ValueError):

        pass

    # pandas Timestamp

    if isinstance(v, pd.Timestamp):

        return v.to_pydatetime()

    # Python datetime/date

    if isinstance(v, (datetime, date)):

        return v

    # Decimal

    if isinstance(v, Decimal):

        return float(v)

    # numpy scalar -> native Python scalar.

    # Most importantly:

    # numpy.int64 -> Python int

    # numpy.float64 -> Python float

    if hasattr(v, "item"):

        return v.item()

    return v

# ============================================================

# DATAFRAME -> MYSQL

# ============================================================

def dataframe_to_records(df):

    """

    IMPORTANT:

    Do NOT use DataFrame.iterrows() here.

    iterrows() creates a Series for each row and can cause

    integer values to be coerced to floating point.

    itertuples() preserves the column scalar types much more

    reliably for BIGINT values.

    """

    columns = list(df.columns)

    records = []

    for row in df.itertuples(

        index=False,

        name=None,

    ):

        record = {

            col: normalize_value(value)

            for col, value in zip(columns, row)

        }

        records.append(record)

    return records

def insert_dataframe(

    engine,

    table,

    df,

    chunk_size=5000,

):

    if df.empty:

        print(f"[LOAD] {table}: 0 rows")

        return

    # Normalize before creating records.

    df = normalize_dataframe_types(

        table,

        df,

    )

    # IMPORTANT:

    # itertuples(), NOT iterrows().

    records = dataframe_to_records(df)

    columns = list(df.columns)

    quoted_cols = ", ".join(

        f"`{c}`"

        for c in columns

    )

    placeholders = ", ".join(

        f":{c}"

        for c in columns

    )

    sql = text(

        f"INSERT INTO `{table}` "

        f"({quoted_cols}) "

        f"VALUES ({placeholders})"

    )

    total = len(records)

    with engine.begin() as conn:

        for start in range(

            0,

            total,

            chunk_size,

        ):

            chunk = records[

                start:start + chunk_size

            ]

            conn.execute(

                sql,

                chunk,

            )

            end = min(

                start + chunk_size,

                total,

            )

            print(

                f"[LOAD] {table}: "

                f"{end:,}/{total:,}"

            )

# ============================================================

# TRUNCATE

# ============================================================

def truncate_in_dependency_order(engine):

    """

    Clear tables from child -> parent.

    FK checks are disabled only during TRUNCATE,

    then immediately enabled again.

    """

    order = [

        "fact_customer_order",

        "fact_reviews",

        "fact_payments",

        "fact_order_items",

        "fact_orders",

        "dim_product",

        "dim_seller",

        "dim_customer",

        "dim_date",

        "dim_category",

        "dim_geolocation",

    ]

    with engine.begin() as conn:

        conn.execute(

            text("SET FOREIGN_KEY_CHECKS=0")

        )

        for table in order:

            conn.execute(

                text(

                    f"TRUNCATE TABLE `{table}`"

                )

            )

        conn.execute(

            text("SET FOREIGN_KEY_CHECKS=1")

        )

    print("[MYSQL] all DW tables truncated")

# ============================================================

# VERIFICATION

# ============================================================

# ============================================================

# VERIFICATION

# ============================================================

def count_rows(engine, table):

    with engine.connect() as conn:

        return conn.execute(

            text(

                f"SELECT COUNT(*) "

                f"FROM `{table}`"

            )

        ).scalar_one()

def verify_bigint_columns(df, table):

    """

    All surrogate/FK key columns must be pandas Int64.

    """

    for col in df.columns:

        if col not in INT_COLUMNS:

            continue

        dtype = str(df[col].dtype)

        if dtype != "Int64":

            raise TypeError(

                f"[TYPE ERROR] {table}.{col}: "

                f"expected pandas Int64, got {dtype}"

            )

    print(f"[TYPE CHECK] {table}: BIGINT columns PASS")

def mysql_key_set(engine, table, column):

    """

    Read a BIGINT key column from MySQL as exact Python ints.

    """

    with engine.connect() as conn:

        rows = conn.execute(

            text(

                f"SELECT `{column}` "

                f"FROM `{table}` "

                f"WHERE `{column}` IS NOT NULL"

            )

        ).fetchall()

    return {int(row[0]) for row in rows}

def compare_key_sets(engine, df, table, column):

    """

    Compare exact non-null key sets between curated and MySQL.

    """

    curated_keys = {

        int(v)

        for v in df[column].dropna().tolist()

    }

    mysql_keys = mysql_key_set(

        engine,

        table,

        column,

    )

    missing_in_mysql = curated_keys - mysql_keys

    extra_in_mysql = mysql_keys - curated_keys

    print(

        f"[KEY VERIFY] {table}.{column}: "

        f"curated_keys={len(curated_keys):,}, "

        f"mysql_keys={len(mysql_keys):,}, "

        f"missing_in_mysql={len(missing_in_mysql):,}, "

        f"extra_in_mysql={len(extra_in_mysql):,}"

    )

    if missing_in_mysql:

        sample = sorted(missing_in_mysql)[:20]

        raise RuntimeError(

            f"{table}.{column}: {len(missing_in_mysql):,} "

            f"curated keys are missing from MySQL. "

            f"First keys: {sample}"

        )

    if extra_in_mysql:

        sample = sorted(extra_in_mysql)[:20]

        raise RuntimeError(

            f"{table}.{column}: {len(extra_in_mysql):,} "

            f"MySQL keys are not present in curated data. "

            f"First keys: {sample}"

        )

    print(

        f"[KEY VERIFY] {table}.{column}: "

        f"EXACT KEY SET PASS"

    )

def verify_foreign_key(

    engine,

    child_df,

    child_table,

    child_column,

    parent_table,

    parent_column,

):

    """

    Exact FK validation BEFORE inserting the child table.

    """

    parent_keys = mysql_key_set(

        engine,

        parent_table,

        parent_column,

    )

    child_keys = {

        int(v)

        for v in child_df[child_column].dropna().tolist()

    }

    orphans = child_keys - parent_keys

    label = (

        f"{child_table}.{child_column} -> "

        f"{parent_table}.{parent_column}"

    )

    if orphans:

        sample = sorted(orphans)[:20]

        print(

            f"[FK CHECK] {label}: "

            f"FAILED - {len(orphans):,} orphan key(s)"

        )

        print(

            f"[FK CHECK] First orphan keys: {sample}"

        )

        raise RuntimeError(

            f"Foreign-key preflight failed for {label}. "

            f"First orphan keys: {sample}"

        )

    print(f"[FK CHECK] {label}: PASS")

def verify_table_row_count(engine, df, table):

    expected_count = len(df)

    actual_count = count_rows(

        engine,

        table,

    )

    print(

        f"[VERIFY] {table}: "

        f"curated={expected_count:,}, "

        f"mysql={actual_count:,}"

    )

    if actual_count != expected_count:

        raise RuntimeError(

            f"Row count mismatch for {table}: "

            f"curated={expected_count}, "

            f"mysql={actual_count}"

        )

# ============================================================

# MAIN LOAD

# ============================================================

def load():

    print("[START] Olist DW Loader")

    print(

        f"[CONFIG] "

        f"MINIO=s3://{MINIO_BUCKET}/"

        f"{CURATED_PREFIX}"

    )

    print(

        f"[CONFIG] "

        f"MYSQL={MYSQL_HOST}:"

        f"{MYSQL_PORT}/"

        f"{MYSQL_DATABASE}"

    )

    print(

        f"[CONFIG] "

        f"LOAD_MODE={LOAD_MODE}"

    )

    s3 = minio_client()

    engine = mysql_engine()

    wait_mysql(engine)

    if LOAD_MODE == "replace":

        truncate_in_dependency_order(engine)

    elif LOAD_MODE != "append":

        raise ValueError(

            "LOAD_MODE must be 'replace' or 'append'"

        )

    for curated_path, table in TABLES:

        print()

        print("=" * 70)

        print(f"[STEP] {curated_path} -> {table}")

        print("=" * 70)

        df = read_curated_table(

            s3,

            curated_path,

        )

        df = normalize_dataframe_types(

            table,

            df,

        )

        verify_bigint_columns(

            df,

            table,

        )

        # Exact FK preflight checks.

        if table == "dim_customer":

            verify_foreign_key(

                engine,

                df,

                "dim_customer",

                "geolocation_key",

                "dim_geolocation",

                "geolocation_key",

            )

        elif table == "dim_seller":

            verify_foreign_key(

                engine,

                df,

                "dim_seller",

                "geolocation_key",

                "dim_geolocation",

                "geolocation_key",

            )

        elif table == "dim_product":

            verify_foreign_key(

                engine,

                df,

                "dim_product",

                "category_key",

                "dim_category",

                "category_key",

            )

        elif table == "fact_orders":

            verify_foreign_key(

                engine,

                df,

                "fact_orders",

                "customer_key",

                "dim_customer",

                "customer_key",

            )

            verify_foreign_key(

                engine,

                df,

                "fact_orders",

                "purchase_date_key",

                "dim_date",

                "date_key",

            )

        elif table == "fact_order_items":

            verify_foreign_key(

                engine,

                df,

                "fact_order_items",

                "order_key",

                "fact_orders",

                "order_key",

            )

            verify_foreign_key(

                engine,

                df,

                "fact_order_items",

                "product_key",

                "dim_product",

                "product_key",

            )

            verify_foreign_key(

                engine,

                df,

                "fact_order_items",

                "seller_key",

                "dim_seller",

                "seller_key",

            )

            verify_foreign_key(

                engine,

                df,

                "fact_order_items",

                "date_key",

                "dim_date",

                "date_key",

            )

        elif table == "fact_payments":

            verify_foreign_key(

                engine,

                df,

                "fact_payments",

                "order_key",

                "fact_orders",

                "order_key",

            )

        elif table == "fact_reviews":

            verify_foreign_key(

                engine,

                df,

                "fact_reviews",

                "order_key",

                "fact_orders",

                "order_key",

            )

        elif table == "fact_customer_order":

            verify_foreign_key(

                engine,

                df,

                "fact_customer_order",

                "customer_key",

                "dim_customer",

                "customer_key",

            )

        insert_dataframe(

            engine,

            table,

            df,

        )

        verify_table_row_count(

            engine,

            df,

            table,

        )

        # Exact key-set verification for dimensions.

        key_columns = {

            "dim_geolocation": "geolocation_key",

            "dim_category": "category_key",

            "dim_customer": "customer_key",

            "dim_seller": "seller_key",

            "dim_product": "product_key",

            "dim_date": "date_key",

        }

        if table in key_columns:

            compare_key_sets(

                engine,

                df,

                table,

                key_columns[table],

            )

    print()

    print("=" * 70)

    print("[FINISHED] Olist DW load completed successfully")

    print("=" * 70)

if __name__ == "__main__":

    load()
