#!/bin/sh
# Non-destructive MinIO bucket bootstrap for the unified stack.
# It only creates required buckets when absent; it never removes or empties buckets.
set -eu

MINIO_HOST="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_USER="${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}"

echo "Waiting for MinIO at ${MINIO_HOST}..."
attempt=0
until mc alias set local "$MINIO_HOST" "$MINIO_USER" "$MINIO_PASS" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then
    echo "ERROR: MinIO did not become ready"
    exit 1
  fi
  sleep 2
done

for bucket in handoff lakehouse; do
  if mc ls "local/${bucket}" >/dev/null 2>&1; then
    echo "Bucket ${bucket} already exists; preserving contents"
  else
    mc mb "local/${bucket}"
    echo "Created bucket ${bucket}"
  fi
done

echo "Non-destructive MinIO initialization complete."
