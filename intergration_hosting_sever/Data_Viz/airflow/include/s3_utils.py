"""MinIO (S3 API) helpers: Parquet / JSON uploads with deterministic keys (overwrite => idempotent retries)."""
import io
import json

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.models import Variable
from botocore.client import Config


def client():
    return boto3.client(
        "s3",
        endpoint_url=Variable.get("MINIO_ENDPOINT"),
        aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("MINIO_SECRET_KEY"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def bucket():
    return Variable.get("MINIO_BUCKET")


def put_parquet(df, key):
    """Write a DataFrame as Parquet. Timestamps are coerced to microseconds (Spark cannot read nanosecond ts)."""
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), buf, compression="snappy",
                   coerce_timestamps="us", allow_truncated_timestamps=True)
    client().put_object(Bucket=bucket(), Key=key, Body=buf.getvalue())


def put_json(obj, key):
    client().put_object(Bucket=bucket(), Key=key, Body=json.dumps(obj, default=str, indent=2).encode(),
                        ContentType="application/json")
