"""MinIO (S3 API) helpers: Parquet / JSON uploads with deterministic keys (overwrite => idempotent retries).

Parquet files follow a fixed per-table schema derived from the OLTP columns (``arrow_schema``), so every batch of a
table has identical column types. Without it, a column that is all NULL in one batch (e.g. order_delivered_customer_date
of freshly placed orders) is written with Arrow's null type and Spark can no longer read all batches together.
"""
import io
import json
from functools import lru_cache

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.models import Variable
from botocore.client import Config
from sqlalchemy import inspect
from sqlalchemy import types as sqltypes


@lru_cache(maxsize=1)
def client():
    return boto3.client(
        "s3",
        endpoint_url=Variable.get("MINIO_ENDPOINT"),
        aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("MINIO_SECRET_KEY"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


@lru_cache(maxsize=1)
def bucket():
    return Variable.get("MINIO_BUCKET")


def _arrow_type(sql_type):
    if isinstance(sql_type, (sqltypes.DateTime, sqltypes.Date)):
        return pa.timestamp("us")
    if isinstance(sql_type, sqltypes.Integer):
        return pa.int64()
    if isinstance(sql_type, (sqltypes.Numeric, sqltypes.Float)):
        return pa.float64()  # DECIMAL money columns are read as float (coerce_float) -> keep float64
    return pa.string()


def arrow_schema(eng, table, extra=()):
    """Arrow schema = OLTP columns of ``table`` (in SELECT * order) + metadata fields ``extra``."""
    fields = [pa.field(c["name"], _arrow_type(c["type"])) for c in inspect(eng).get_columns(table)]
    return pa.schema(fields + list(extra))


def put_parquet(df, key, schema=None):
    """Write a DataFrame as Parquet (with ``schema`` when given). Timestamps are microseconds (Spark cannot read ns)."""
    buf = io.BytesIO()
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, buf, compression="snappy",
                   coerce_timestamps="us", allow_truncated_timestamps=True)
    client().put_object(Bucket=bucket(), Key=key, Body=buf.getvalue())


def put_json(obj, key):
    client().put_object(Bucket=bucket(), Key=key, Body=json.dumps(obj, default=str, indent=2).encode(),
                        ContentType="application/json")


def list_keys(prefix):
    """Every object key under ``prefix`` (paginated). Pass a prefix ending in '/' to stay inside one folder."""
    keys = []
    for page in client().get_paginator("list_objects_v2").paginate(Bucket=bucket(), Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def delete_keys(keys):
    """Delete objects in batches of 1000 (S3 DeleteObjects limit); raise if MinIO reports any failure."""
    keys = list(keys)
    for i in range(0, len(keys), 1000):
        batch = [{"Key": k} for k in keys[i:i + 1000]]
        errors = client().delete_objects(Bucket=bucket(), Delete={"Objects": batch, "Quiet": True}).get("Errors")
        if errors:
            raise RuntimeError(f"could not delete {len(errors)} objects, first: {errors[0]}")


def read_parquet(key, columns=None):
    """Read one Parquet object (only ``columns`` when given) into an Arrow table."""
    body = client().get_object(Bucket=bucket(), Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body), columns=columns)
