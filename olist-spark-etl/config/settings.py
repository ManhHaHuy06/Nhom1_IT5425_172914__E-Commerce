import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "host.docker.internal:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "olist")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

INPUT_PREFIX = os.getenv("INPUT_PREFIX", "raw").strip("/")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "curated").strip("/")
QUARANTINE_PREFIX = os.getenv("QUARANTINE_PREFIX", "quarantine").strip("/")

S3A_ENDPOINT = f"http{'s' if MINIO_SECURE else ''}://{MINIO_ENDPOINT}"
S3A_ROOT = f"s3a://{MINIO_BUCKET}"