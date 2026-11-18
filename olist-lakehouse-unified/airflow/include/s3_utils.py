"""S3-compatible helpers for the already-existing Lakehouse MinIO bucket."""
import json
import os

import boto3
from airflow.models import Variable
from botocore.client import Config


def client():
    return boto3.client(
        "s3",
        endpoint_url=Variable.get("MINIO_ENDPOINT"),
        aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("MINIO_SECRET_KEY"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def bucket():
    return Variable.get("MINIO_BUCKET", default_var="lakehouse")


def verify_bucket():
    # Never create or initialize a bucket here. It must already exist in Lakehouse MinIO.
    client().head_bucket(Bucket=bucket())


def upload_file(local_path: str, key: str):
    client().upload_file(local_path, bucket(), key)


def put_json(obj, key: str):
    client().put_object(
        Bucket=bucket(), Key=key,
        Body=json.dumps(obj, ensure_ascii=False, default=str, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def object_uri(key: str) -> str:
    return f"s3://{bucket()}/{key}"


def clear_staging_prefix(prefix: str):
    """Clear only one owned staging prefix before retrying an upload.

    This prevents stale Parquet part files from a prior partial attempt being
    read together with a smaller retry snapshot. Managed Iceberg prefixes are
    never accepted by this helper.
    """
    normalized = prefix.strip("/")
    if not normalized.startswith("staging/bronze/"):
        raise ValueError(f"Refusing to clear non-staging prefix: {normalized}")
    s3 = client()
    pager = s3.get_paginator("list_objects_v2")
    for page in pager.paginate(Bucket=bucket(), Prefix=normalized + "/"):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        for start in range(0, len(objects), 1000):
            batch = objects[start:start + 1000]
            if batch:
                result = s3.delete_objects(Bucket=bucket(), Delete={"Objects": batch, "Quiet": True})
                errors = result.get("Errors", [])
                if errors:
                    raise RuntimeError(f"Could not clear staging objects: {errors[:5]}")
